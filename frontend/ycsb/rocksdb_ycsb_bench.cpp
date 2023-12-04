#include <gflags/gflags.h>
#include <immintrin.h>

#include <condition_variable>  // std::condition_variable
#include <cstdlib>
#include <mutex>   // std::mutex
#include <thread>  // std::thread

#include "Units.hpp"
#include "XYStore.hpp"
#include "histogram.h"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ScrambledZipfGenerator.hpp"
#include "logger.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "slice.h"
#include "test_util.h"
#include "trace.h"

using namespace leanstore;

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

using namespace util;
using KeyTrace = RandomKeyTrace<util::TraceUniform>;
using YCSBKey = u64;
using YCSBPayload = BytesPayload<8>;

DEFINE_uint32 (batch, 100, "report batch");
DEFINE_uint32 (readtime, 0, "if 0, then we read all keys");
DEFINE_uint64 (report_interval, 0, "Report interval in seconds");
DEFINE_uint64 (stats_interval, 100000000, "Report interval in ops");
DEFINE_uint64 (value_size, 8, "The value size");
DEFINE_uint64 (num, 80 * 1000000LU, "Number of total record");
DEFINE_uint64 (read, 0, "Number of read operations");
DEFINE_uint64 (write, 0, "Number of read operations");
DEFINE_bool (hist, false, "");
DEFINE_string (benchmarks, "load,readall", "");
DEFINE_string (tracefile, "randomtrace.data", "");
DEFINE_double (percentile, 90, "percentile of exponential distribution for read");
DEFINE_bool (is_seq, false, "enable the sequential trace");

namespace {

void getInt (u8* dst, u64 size, u64 val) {
    u8* src = reinterpret_cast<u8*> (&val);
    for (u64 t_i = 0; t_i < size; t_i++) {
        dst[t_i] = src[t_i];
    }
    // std::cout << *reinterpret_cast<uint64_t*> (dst) << std::endl;
}

static void TransformKey (uint64_t rid, IndexKey& index_key) {
    index_key.setKeyLen (sizeof (rid));
    reinterpret_cast<uint64_t*> (&index_key[0])[0] = __builtin_bswap64 (rid);
}

class Stats {
public:
    int tid_;
    double start_;
    double finish_;
    double seconds_;
    double next_report_time_;
    double last_op_finish_;
    unsigned last_level_compaction_num_;
    Histogram hist_;

    uint64_t done_;
    uint64_t last_report_done_;
    uint64_t last_report_finish_;
    uint64_t next_report_;
    std::string message_;

    Stats () { Start (); }
    explicit Stats (int id) : tid_ (id) { Start (); }

    void Start () {
        start_ = NowMicros ();
        next_report_time_ = start_ + FLAGS_report_interval * 1000000;
        next_report_ = 100;
        last_op_finish_ = start_;
        last_report_done_ = 0;
        last_report_finish_ = start_;
        last_level_compaction_num_ = 0;
        done_ = 0;
        seconds_ = 0;
        finish_ = start_;
        message_.clear ();
        hist_.Clear ();
    }

    void Merge (const Stats& other) {
        hist_.Merge (other.hist_);
        done_ += other.done_;
        seconds_ += other.seconds_;
        if (other.start_ < start_) start_ = other.start_;
        if (other.finish_ > finish_) finish_ = other.finish_;

        // Just keep the messages from one thread
        if (message_.empty ()) message_ = other.message_;
    }

    void Stop () {
        finish_ = NowMicros ();
        seconds_ = (finish_ - start_) * 1e-6;
        ;
    }

    void StartSingleOp () { last_op_finish_ = NowMicros (); }

    void PrintSpeed () {
        uint64_t now = NowMicros ();
        int64_t usecs_since_last = now - last_report_finish_;

        std::string cur_time = TimeToString (now / 1000000);
        // printf (
        //     "%s ... thread %d: (%lu,%lu) ops and "
        //     "( %.1f,%.1f ) ops/second in (%.4f,%.4f) seconds\n",
        //     cur_time.c_str (), tid_, done_ - last_report_done_, done_,
        //     (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
        //     done_ / ((now - start_) / 1000000.0), (now - last_report_finish_) / 1000000.0,
        //     (now - start_) / 1000000.0);
        INFO ("[Epoch] %d,%lu,%lu,%.4f,%.4f\n", tid_, done_ - last_report_done_, done_,
              (now - last_report_finish_) / 1000000.0, (now - start_) / 1000000.0);
        INFO (
            "%s ... thread %d: (%lu,%lu) ops and "
            "( %.1f,%.1f ) ops/second in (%.6f,%.6f) seconds\n",
            cur_time.c_str (), tid_, done_ - last_report_done_, done_,
            (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
            done_ / ((now - start_) / 1000000.0), (now - last_report_finish_) / 1000000.0,
            (now - start_) / 1000000.0);
        last_report_finish_ = now;
        last_report_done_ = done_;
        fflush (stdout);
    }

    static void AppendWithSpace (std::string* str, const std::string& msg) {
        if (msg.empty ()) return;
        if (!str->empty ()) {
            str->push_back (' ');
        }
        str->append (msg.data (), msg.size ());
    }

    void AddMessage (const std::string& msg) { AppendWithSpace (&message_, msg); }

    inline bool FinishedBatchOp (size_t batch) {
        double now = NowNanos ();
        last_op_finish_ = now;
        done_ += batch;
        if ((done_ >= next_report_)) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            fprintf (stderr, "... finished %llu ops%30s\r", (unsigned long long)done_, "");

            if (FLAGS_report_interval == 0 && (done_ % FLAGS_stats_interval) == 0) {
                PrintSpeed ();
                return 0;
            }
            fflush (stderr);
            fflush (stdout);
        }

        if (FLAGS_report_interval != 0 && NowMicros () > next_report_time_) {
            next_report_time_ += FLAGS_report_interval * 1000000;
            PrintSpeed ();
            return 1;
        }
        return 0;
    }

    inline void FinishedSingleOp () {
        double now = NowNanos ();
        last_op_finish_ = now;

        done_++;
        if (done_ >= next_report_) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            fprintf (stderr, "... finished %llu ops%30s\r", (unsigned long long)done_, "");

            if (FLAGS_report_interval == 0 && (done_ % FLAGS_stats_interval) == 0) {
                PrintSpeed ();
                return;
            }
            fflush (stderr);
            fflush (stdout);
        }

        if (FLAGS_report_interval != 0 && NowMicros () > next_report_time_) {
            next_report_time_ += FLAGS_report_interval * 1000000;
            PrintSpeed ();
        }
    }

    std::string TimeToString (uint64_t secondsSince1970) {
        const time_t seconds = (time_t)secondsSince1970;
        struct tm t;
        int maxsize = 64;
        std::string dummy;
        dummy.reserve (maxsize);
        dummy.resize (maxsize);
        char* p = &dummy[0];
        localtime_r (&seconds, &t);
        snprintf (p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900, t.tm_mon + 1,
                  t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
        return dummy;
    }

    void Report (const util::Slice& name, bool print_hist = false) {
        // Pretend at least one op was done in case we are running a benchmark
        // that does not call FinishedSingleOp().
        if (done_ < 1) done_ = 1;

        std::string extra;

        AppendWithSpace (&extra, message_);

        double elapsed = (finish_ - start_) * 1e-6;

        double throughput = (double)done_ / elapsed;

        printf ("%-12s : %11.3f micros/op %lf Mops/s;%s%s\n", name.ToString ().c_str (),
                elapsed * 1e6 / done_, throughput / 1024 / 1024, (extra.empty () ? "" : " "),
                extra.c_str ());
        INFO ("%-12s : %11.3f micros/op %lf Mops/s;%s%s \n", name.ToString ().c_str (),
              elapsed * 1e6 / done_, throughput / 1024 / 1024, (extra.empty () ? "" : " "),
              extra.c_str ());
        if (print_hist) {
            fprintf (stdout, "Nanoseconds per op:\n%s\n", hist_.ToString ().c_str ());
        }

        fflush (stdout);
        fflush (stderr);
    }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
    std::mutex mu;
    std::condition_variable cv;
    int total;

    // Each thread goes through the following states:
    //    (1) initializing
    //    (2) waiting for others to be initialized
    //    (3) running
    //    (4) done

    int num_initialized;
    int num_done;
    bool start;

    SharedState (int total) : total (total), num_initialized (0), num_done (0), start (false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
    int tid;  // 0..n-1 when running in n threads
    // Random rand;         // Has different seeds for different threads
    Stats stats;
    SharedState* shared;
    YCSBGenerator ycsb_gen;
    ThreadState (int index) : tid (index), stats (index) {}
};

class Duration {
public:
    Duration (uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
        max_seconds_ = max_seconds;
        max_ops_ = max_ops;
        ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
        ops_ = 0;
        start_at_ = NowMicros ();
    }

    inline int64_t GetStage () { return std::min (ops_, max_ops_ - 1) / ops_per_stage_; }

    inline bool Done (int64_t increment) {
        if (increment <= 0) increment = 1;  // avoid Done(0) and infinite loops
        ops_ += increment;

        if (max_seconds_) {
            // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
            auto granularity = 1000;
            if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
                uint64_t now = NowMicros ();
                return ((now - start_at_) / 1000000) >= max_seconds_;
            } else {
                return false;
            }
        } else {
            return ops_ > max_ops_;
        }
    }

    inline int64_t Ops () { return ops_; }

private:
    uint64_t max_seconds_;
    int64_t max_ops_;
    int64_t ops_per_stage_;
    int64_t ops_;
    uint64_t start_at_;
};

#if defined(__linux)
static std::string TrimSpace (std::string s) {
    size_t start = 0;
    while (start < s.size () && isspace (s[start])) {
        start++;
    }
    size_t limit = s.size ();
    while (limit > start && isspace (s[limit - 1])) {
        limit--;
    }
    return std::string (s.data () + start, limit - start);
}
#endif

}  // namespace

template <typename KeyTrace>
class Benchmark {
public:
    uint64_t num_;
    int value_size_;
    size_t reads_;
    size_t writes_;
    KeyTrace* key_trace_;
    size_t trace_size_;
    size_t initial_capacity_;

    // LeanStore DB
    rocksdb::DB* db;
    rocksdb::Options options;
    rocksdb::DBOptions dboptions;
    rocksdb::WriteOptions woptions;
    rocksdb::ReadOptions roptions;
    rocksdb::BlockBasedTableOptions tableOptions;
    int currRealMem = 0, peakRealMem = 0, currVirtMem = 0, peakVirtMem = 0;

    Benchmark ()
        : num_ (FLAGS_num),
          value_size_ (FLAGS_value_size),
          reads_ (FLAGS_read),
          writes_ (FLAGS_write),
          key_trace_ (nullptr) {
        woptions.disableWAL = true;
        woptions.sync = false;

        // Enable the bloomfilter
        tableOptions.whole_key_filtering = true;
        tableOptions.filter_policy.reset (rocksdb::NewBloomFilterPolicy (10, false));
        tableOptions.block_cache = rocksdb::NewLRUCache (512 * 1024 * 1024LL);
        // tableOptions.cache_index_and_filter_blocks = true;
        // tableOptions.pin_l0_filter_and_index_blocks_in_cache = true;

        // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
        options.IncreaseParallelism (4);
        options.OptimizeLevelStyleCompaction ();
        // ====================== FLATTEN the throughput ========================
        // Set up the rate limiter to allow a maximum of 1 MB/s write rate
        // The first parameter is the write rate limit in bytes per second
        // The second parameter is the refill period in microseconds
        // The third parameter is the fair queueing factor (optional)
        // options.rate_limiter.reset (rocksdb::NewGenericRateLimiter (150000000));

        options.use_direct_io_for_flush_and_compaction = true;
        options.use_direct_reads = true;
        options.create_if_missing = true;
        options.max_open_files = -1;
        options.compression = rocksdb::kNoCompression;
        options.compaction_style = rocksdb::kCompactionStyleUniversal;
        options.write_buffer_size = 256 * 1024 * 1024;
        // options.arena_block_size = 64 * 1024;
        // options.disable_auto_compactions = true;
        options.table_factory.reset (rocksdb::NewBlockBasedTableFactory (tableOptions));

        // create the database

        rocksdb::Status s = rocksdb::DB::Open (options, FLAGS_ssd_path, &db);
    }
    ~Benchmark () { delete key_trace_; }
    void Run () {
        trace_size_ = FLAGS_num;
        key_trace_ = new KeyTrace (trace_size_, FLAGS_is_seq);
        num_ = key_trace_->keys_.size ();
        FLAGS_num = key_trace_->keys_.size ();
        trace_size_ = key_trace_->keys_.size ();
        if (reads_ == 0) {
            reads_ = key_trace_->keys_.size ();
        }
        PrintHeader ();
        // run benchmark
        bool print_hist = false;
        const char* benchmarks = FLAGS_benchmarks.c_str ();
        while (benchmarks != nullptr) {
            int thread = FLAGS_worker_threads;
            void (Benchmark::*method) (ThreadState*) = nullptr;
            const char* sep = strchr (benchmarks, ',');
            std::string name;
            if (sep == nullptr) {
                name = benchmarks;
                benchmarks = nullptr;
            } else {
                name = std::string (benchmarks, sep - benchmarks);
                benchmarks = sep + 1;
            }
            if (name == "load") {
                // key_trace_->Randomize ();
                method = &Benchmark::DoWrite;
            } else if (name == "readall") {
                method = &Benchmark::DoReadAll;
            } else if (name == "readtrace") {
                thread = 1;
                method = &Benchmark::DoReadTrace;
            } else if (name == "savetrace") {
                thread = 1;
                method = &Benchmark::DoSaveTrace;
            }

            if (method != nullptr) RunBenchmark (thread, name, method, print_hist);
        }
    }

    void DoWrite (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            ERROR ("DoWrite lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_worker_threads;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);

        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                auto& key = key_iterator.Next ();
                Key k;
                TransformKey (key, k);

                std::string rValue;
                rValue.resize (8);
                std::memcpy (rValue.data (), &key, 8);
                db->Put (woptions, rocksdb::Slice ((char*)k.getData (), k.getKeyLen ()), rValue);
            }
            auto ret = thread->stats.FinishedBatchOp (j);
        }
    }

    void DoReadAll (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("DoReadAll lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_worker_threads;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);

        size_t not_find = 0;
        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();
        while (!duration.Done (batch) && key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t ikey = key_iterator.Next ();
                Key k;
                TransformKey (ikey, k);

                std::string rValue;
                auto isok = db->Get (roptions, rocksdb::Slice ((char*)k.getData (), k.getKeyLen ()),
                                     &rValue);
                if (!isok.ok ()) {
                    not_find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", interval, not_find);
        if (not_find)
            INFO ("thread %2d num: %lu, not find: %lu\n", thread->tid, interval, not_find);
        thread->stats.AddMessage (buf);
    }

    void DoSaveTrace (ThreadState* thread) {
        auto starttime = std::chrono::system_clock::now ();
        key_trace_->ToFile (FLAGS_tracefile);
        auto duration = std::chrono::duration_cast<std::chrono::microseconds> (
            std::chrono::system_clock::now () - starttime);
    }

    void DoReadTrace (ThreadState* thread) {
        auto starttime = std::chrono::system_clock::now ();
        uint64_t sample_length = 1;
        uint64_t working_set_length = reads_;
        key_trace_->FromFile (FLAGS_tracefile);
        key_trace_->Randomize ();
        num_ = key_trace_->keys_.size ();
        FLAGS_num = key_trace_->keys_.size ();
        trace_size_ = key_trace_->keys_.size ();
        if (reads_ == 0) {
            reads_ = key_trace_->read_keys_.size ();
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds> (
            std::chrono::system_clock::now () - starttime);
    }

private:
    struct ThreadArg {
        Benchmark* bm;
        SharedState* shared;
        ThreadState* thread;
        void (Benchmark::*method) (ThreadState*);
    };

    static void ThreadBody (void* v) {
        ThreadArg* arg = reinterpret_cast<ThreadArg*> (v);
        SharedState* shared = arg->shared;
        ThreadState* thread = arg->thread;
        {
            std::unique_lock<std::mutex> lck (shared->mu);
            shared->num_initialized++;
            if (shared->num_initialized >= shared->total) {
                shared->cv.notify_all ();
            }
            while (!shared->start) {
                shared->cv.wait (lck);
            }
        }

        thread->stats.Start ();
        (arg->bm->*(arg->method)) (thread);
        thread->stats.Stop ();

        {
            std::unique_lock<std::mutex> lck (shared->mu);
            shared->num_done++;
            if (shared->num_done >= shared->total) {
                shared->cv.notify_all ();
            }
        }
    }

    void RunBenchmark (int thread_num, const std::string& name,
                       void (Benchmark::*method) (ThreadState*), bool print_hist) {
        SharedState shared (thread_num);
        ThreadArg* arg = new ThreadArg[thread_num];
        std::thread server_threads[thread_num];
        for (int i = 0; i < thread_num; i++) {
            arg[i].bm = this;
            arg[i].method = method;
            arg[i].shared = &shared;
            arg[i].thread = new ThreadState (i);
            arg[i].thread->shared = &shared;
            server_threads[i] = std::thread (ThreadBody, &arg[i]);
        }

        std::unique_lock<std::mutex> lck (shared.mu);
        while (shared.num_initialized < thread_num) {
            shared.cv.wait (lck);
        }

        shared.start = true;
        shared.cv.notify_all ();
        while (shared.num_done < thread_num) {
            shared.cv.wait (lck);
        }

        for (int i = 1; i < thread_num; i++) {
            arg[0].thread->stats.Merge (arg[i].thread->stats);
        }
        arg[0].thread->stats.Report (name, print_hist);

        for (auto& th : server_threads) th.join ();

        for (int i = 0; i < thread_num; i++) {
            delete arg[i].thread;
        }
        delete[] arg;
    }

    void PrintEnvironment () {
#if defined(__linux)
        time_t now = time (nullptr);
        fprintf (stderr, "Date:                  %s", ctime (&now));  // ctime() adds newline

        FILE* cpuinfo = fopen ("/proc/cpuinfo", "r");
        if (cpuinfo != nullptr) {
            char line[1000];
            int num_cpus = 0;
            std::string cpu_type;
            std::string cache_size;
            while (fgets (line, sizeof (line), cpuinfo) != nullptr) {
                const char* sep = strchr (line, ':');
                if (sep == nullptr) {
                    continue;
                }
                std::string key = TrimSpace (std::string (line, sep - 1 - line));
                std::string val = TrimSpace (std::string (sep + 1));
                if (key == "model name") {
                    ++num_cpus;
                    cpu_type = val;
                } else if (key == "cache size") {
                    cache_size = val;
                }
            }
            fclose (cpuinfo);
            fprintf (stderr, "CPU:                   %d * %s\n", num_cpus, cpu_type.c_str ());
            fprintf (stderr, "CPUCache:              %s\n", cache_size.c_str ());
        }
#endif
    }

    void PrintHeader () {
        INFO ("------------------------------------------------\n");
        fprintf (stdout, "------------------------------------------------\n");
        PrintEnvironment ();
        fprintf (stdout, "LEANSTORE_YCSB\n");
        INFO ("LEANSTORE_YCSB \n");
        fprintf (stdout, "Trace:                 %s\n", KeyTrace::Name ().c_str ());
        INFO ("Trace:                 %s\n", KeyTrace::Name ().c_str ());
        fprintf (stdout, "val size:              %d\n", 8);
        INFO ("val size:              %d\n", 8);
        fprintf (stdout, "Entries:               %lu\n", (uint64_t)num_);
        INFO ("Entries:               %lu\n", (uint64_t)num_);
        fprintf (stdout, "Trace size:            %lu\n", (uint64_t)trace_size_);
        INFO ("Trace size:            %lu\n", (uint64_t)trace_size_);
        fprintf (stdout, "Read:                  %lu \n", (uint64_t)FLAGS_read);
        INFO ("Read:                  %lu \n", (uint64_t)FLAGS_read);
        fprintf (stdout, "Write:                 %lu \n", (uint64_t)FLAGS_write);
        INFO ("Write:                 %lu \n", (uint64_t)FLAGS_write);
        fprintf (stdout, "Thread:                %lu \n", (uint64_t)FLAGS_worker_threads);
        INFO ("Thread:                %lu \n", (uint64_t)FLAGS_worker_threads);
        fprintf (stdout, "Report interval:       %lu s\n", (uint64_t)FLAGS_report_interval);
        INFO ("Report interval:       %lu s\n", (uint64_t)FLAGS_report_interval);
        fprintf (stdout, "Stats interval:        %lu records\n", (uint64_t)FLAGS_stats_interval);
        INFO ("Stats interval:        %lu records\n", (uint64_t)FLAGS_stats_interval);
        fprintf (stdout, "benchmarks:            %s\n", FLAGS_benchmarks.c_str ());
        INFO ("benchmarks:            %s\n", FLAGS_benchmarks.c_str ());
        fprintf (stdout, "------------------------------------------------\n");
        INFO ("------------------------------------------------\n");
    }
};

int main (int argc, char* argv[]) {
    ParseCommandLineFlags (&argc, &argv, true);
    Benchmark<KeyTrace> benchmark;
    benchmark.Run ();
    return 0;
}