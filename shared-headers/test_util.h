#pragma once

#include <logger.h>
#include <sys/time.h>
#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_sort.h>

#include <algorithm>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "trace.h"
#include "zipfian_int_distribution.h"

#define KEY_LEN ((16))

// Returns the number of micro-seconds since some fixed point in time. Only
// useful for computing deltas of time.
inline uint64_t NowMicros () {
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct ::timeval tv;
    ::gettimeofday (&tv, nullptr);
    return static_cast<uint64_t> (tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
}
// Returns the number of nano-seconds since some fixed point in time. Only
// useful for computing deltas of time in one run.
// Default implementation simply relies on NowMicros.
// In platform-specific implementations, NowNanos() should return time points
// that are MONOTONIC.
inline uint64_t NowNanos () {
    struct timespec ts;
    clock_gettime (CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t> (ts.tv_sec) * 1000000000L + ts.tv_nsec;
}

static constexpr uint64_t kRandNumMax = (1LU << 60);
static constexpr uint64_t kRandNumMaxMask = kRandNumMax - 1;

static uint64_t u64Rand (const uint64_t& min, const uint64_t& max) {
    static thread_local std::mt19937 generator (std::random_device{}());
    std::uniform_int_distribution<uint64_t> distribution (min, max);
    return distribution (generator);
}

void getMemory (int* currRealMem, int* peakRealMem, int* currVirtMem, int* peakVirtMem) {
    // stores each word in status file
    char buffer[1024] = "";

    // linux file contains this-process info
    FILE* file = fopen ("/proc/self/status", "r");

    // read the entire file
    while (fscanf (file, " %1023s", buffer) == 1) {
        if (strcmp (buffer, "VmRSS:") == 0) {
            fscanf (file, " %d", currRealMem);
        }
        if (strcmp (buffer, "VmHWM:") == 0) {
            fscanf (file, " %d", peakRealMem);
        }
        if (strcmp (buffer, "VmSize:") == 0) {
            fscanf (file, " %d", currVirtMem);
        }
        if (strcmp (buffer, "VmPeak:") == 0) {
            fscanf (file, " %d", peakVirtMem);
        }
    }
    fclose (file);
}

template <class TraceType>
class RandomKeyTrace {
public:
    util::TraceUniform trace;

public:
    RandomKeyTrace (size_t count, bool is_seq = false, bool prepared_trace = true) {
        if (prepared_trace) {
            count_ = count;
            keys_.resize (count);

            printf ("generate %lu keys, is_seq %d\n", count, is_seq);
            auto starttime = std::chrono::system_clock::now ();
            for (size_t i = 0; i < count; i++) {
                if (!is_seq) {
                    keys_[i] = trace.Next ();
                } else {
                    keys_[i] = i + 1;
                }
            }

            printf ("Ensure there is no duplicate keys\n");
            tbb::parallel_sort (keys_.begin (), keys_.end ());
            auto it = std::unique (keys_.begin (), keys_.end ());
            keys_.erase (it, keys_.end ());
            printf ("%lu keys left \n", keys_.size ());

            auto duration = std::chrono::duration_cast<std::chrono::microseconds> (
                std::chrono::system_clock::now () - starttime);
            printf ("generate duration %f s.\n", duration.count () / 1000000.0);

            size_t vector_overhead = sizeof (std::vector<uint64_t>);
            size_t element_memory = keys_.size () * sizeof (uint64_t);

            printf ("key space usage %2.10f GB\n", element_memory / 1024.0 / 1024 / 1024);
        }
    }

    void ToFile (const std::string& filename) {
        std::ofstream outfile (filename, std::ios::out | std::ios_base::binary);
        for (size_t k : keys_) {
            outfile.write (reinterpret_cast<char*> (&k), 8);
            // printf ("out key %016lu\n", k);
        }
        outfile.close ();
    }

    void FromFile (const std::string& filename) {
        std::ifstream infile (filename, std::ios::in | std::ios_base::binary);
        size_t key;
        keys_.clear ();
        while (!infile.eof ()) {
            infile.read (reinterpret_cast<char*> (&key), 8);
            keys_.push_back (key);
            // printf (" in key %016lu\n", key);
        }
        count_ = keys_.size ();
        infile.close ();
        INFO ("Read From File %lu\n", count_);
    }

    void CreateReadWorkloadWithZipfianDistribution (uint64_t read_dataset_length,
                                                    double distrib_param) {
        std::default_random_engine generator;
        zipfian_int_distribution<int> distribution (1, count_, distrib_param);
        INFO ("[YCSB] Zipfian constant %2.2f\n", distrib_param);
        read_keys_.resize (read_dataset_length);
        read_i_.resize (read_dataset_length);
        std::unordered_map<uint64_t, uint64_t> umap;
        for (size_t j = 0; j < read_dataset_length; j++) {
            read_i_[j] = distribution (generator);
        }
        std::random_shuffle (keys_.begin (), keys_.end ());  // Randomize the keys
        for (size_t j = 0; j < read_dataset_length; j++) {
            read_keys_[j] = keys_[read_i_[j]];
            umap[read_i_[j]] += 1;
        }
        // ############################################################################################
        uint64_t count20 = 0, count40 = 0, count60 = 0, count80 = 0, count100 = 0;
        for (auto& item : umap) {
            if (item.first < count_ * 0.2) count20 += item.second;
            if (item.first < count_ * 0.4) count40 += item.second;
            if (item.first < count_ * 0.6) count60 += item.second;
            if (item.first < count_ * 0.8) count80 += item.second;
            if (item.first < count_ * 1.0) count100 += item.second;
        }
        INFO ("[YCSB Data Distribution] 20 %2.3f 40 %2.3f 60 %2.3f 80 %2.3f 100 %2.3f\n",
              count20 * 1.0 / count100, count40 * 1.0 / count100, count60 * 1.0 / count100,
              count80 * 1.0 / count100, count100 * 1.0 / count100);
        INFO ("[YCSB] Unique number for generated workload %lu\n", umap.size ());
    }

    void CreateReadWorkloadWithInterval (uint64_t read_dataset_length, uint64_t interval,
                                         double distrib_param) {
        // 0-interval-interval*2 ... -1-1+interval-1+2*interval-...
        read_keys_.resize (read_dataset_length);
        uint64_t cur_working_set_len = 0;
        for (uint64_t j = 0; j < interval; j++) {
            for (uint64_t k = 0; k < count_ / interval; k++) {
                if (cur_working_set_len < read_dataset_length) {
                    read_keys_[cur_working_set_len] = keys_[j + k * interval];
                    cur_working_set_len++;
                    if (cur_working_set_len >= read_dataset_length) break;
                }
            }
            if (cur_working_set_len >= read_dataset_length) break;
        }
    }

    void CreateReadWorkloadWithLocalSequential4Workloads (uint64_t read_dataset_length,
                                                          uint64_t segment_length,
                                                          double distrib_param) {
        read_keys_distribution_v1.resize (read_dataset_length);
        read_keys_distribution_v2.resize (read_dataset_length);
        read_keys_distribution_v3.resize (read_dataset_length);
        read_keys_distribution_v4.resize (read_dataset_length);

        // 1. split the read/write dataset with specific segment length
        uint64_t keyspace_length = count_;
        uint64_t temp_read_dataset_length = read_dataset_length / segment_length;
        uint64_t nSegments = keyspace_length / segment_length;
        std::vector<uint64_t> segmentIndex;
        segmentIndex.resize (nSegments);
        // segment i means that [i, i + segment_length)
        for (size_t idx = 0; idx < nSegments; idx++) {
            segmentIndex[idx] = idx;
        }

        // 2. randomize the segment index only
        std::default_random_engine generator;
        zipfian_int_distribution<int> distribution (1, nSegments, distrib_param);
        INFO ("[WorkloadShifting] Zipfian constant %2.2f KeySpace %lu nSegments %lu\n", distrib_param,
              keyspace_length, nSegments);
        read_keys_.resize (read_dataset_length);
        read_i_.resize (temp_read_dataset_length);

        std::random_shuffle (segmentIndex.begin (), segmentIndex.end ());  // Randomize the keys
        std::unordered_map<uint64_t, uint64_t> umap;
        for (size_t j = 0; j < temp_read_dataset_length; j++) {
            read_i_[j] = distribution (generator);
            // umap[segmentIndex[read_i_[j] - 1]] += 1;
        }
        // ############################################################################################
        // uint64_t count20 = 0, count40 = 0, count60 = 0, count80 = 0, count100 = 0;
        // for (auto& item : umap) {
        //     if (item.first < nSegments * 0.2) count20 += item.second;
        //     if (item.first < nSegments * 0.4) count40 += item.second;
        //     if (item.first < nSegments * 0.6) count60 += item.second;
        //     if (item.first < nSegments * 0.8) count80 += item.second;
        //     if (item.first < nSegments * 1.0) count100 += item.second;
        // }
        // INFO ("[WorkloadShifting Data Distribution] 20 %2.3f 40 %2.3f 60 %2.3f 80 %2.3f 100 %2.3f\n",
        //       count20 * 1.0 / count100, count40 * 1.0 / count100, count60 * 1.0 / count100,
        //       count80 * 1.0 / count100, count100 * 1.0 / count100);
        // INFO ("[WorkloadShifting] Unique number for generated workload %lu\n", umap.size ());

        // INFO ("[WorkloadShifting] Prepared read_i_");
        // for (size_t j = 0; j < temp_read_dataset_length; j++) {
        //     uint64_t start =
        //         (segmentIndex[read_i_[j] - 1]) * segment_length;  // start position of key vector
        //     for (size_t k = 0; k < segment_length; k++) {
        //         read_keys_[j * segment_length + k] = keys_[start + k];
        //     }
        // }
        // INFO ("[WorkloadShifting] Prepared whole read workload");

        // Randomize read_i_
        // std::random_shuffle (read_i_.begin (), read_i_.end ());

        INFO ("temp_read_dataset_length %llu", temp_read_dataset_length);
        // for (size_t j = 0; j < temp_read_dataset_length; j++) {
        //     uint64_t start =
        //         (segmentIndex[read_i_[j] - 1]) * segment_length;  // start position of key vector
        //     for (size_t k = 0; k < segment_length; k++) {
        //         read_keys_distribution_v1[j * segment_length + k] = keys_[start + k];
        //     }
        // }
        for (size_t j = 0; j < temp_read_dataset_length; j++) {
            uint64_t start =
                (segmentIndex[read_i_[j] - 1]) * segment_length;  // start position of key vector
            for (size_t k = 0; k < segment_length; k++) {
                read_keys_distribution_v1[j * segment_length + k] = keys_[start + k];
            }
        }
        INFO ("Phase 1 prepared");
        // INFO ("phase_length %lu temp %llu read_keys_distribution_v1 %lu", phase_length, temp.size(), read_keys_distribution_v1.size());

        uint64_t phase_length = keys_.size () / 4;
        // Temporary storage for the 4th phase
        std::vector<uint64_t> temp;
        temp.reserve(phase_length);
        
        // temp.insert (temp.end (), keys_.begin () + 3 * phase_length, keys_.end ());
        // // Shift phases 1, 2, 3 to the right
        // std::move_backward (keys_.begin (), keys_.begin () + 3 * phase_length, keys_.end ());
        // // Copy 4th phase to the beginning
        // std::copy (temp.begin (), temp.end (), keys_.begin ());

        // Shifting phases 1, 2, 3 to the right and preparing phase 4
        temp.assign(keys_.begin() + 3 * phase_length, keys_.end());
        std::move_backward(keys_.begin(), keys_.begin() + 3 * phase_length, keys_.end());
        std::copy(temp.begin(), temp.end(), keys_.begin());

        INFO ("keys_ size %lu temp %lu", keys_.size(), temp.size());

        for (size_t j = 0; j < temp_read_dataset_length; j++) {
            uint64_t start =
                (segmentIndex[read_i_[j] - 1]) * segment_length;  // start position of key vector
            for (size_t k = 0; k < segment_length; k++) {
                read_keys_distribution_v2[j * segment_length + k] = keys_[start + k];
            }
        }
        INFO ("Phase 2 prepared");
        temp.clear ();
        // temp.insert (temp.end (), keys_.begin () + 3 * phase_length, keys_.end ());
        // // Shift phases 1, 2, 3 to the right
        // std::move_backward (keys_.begin (), keys_.begin () + 3 * phase_length, keys_.end ());
        // // Copy 4th phase to the beginning
        // std::copy (temp.begin (), temp.end (), keys_.begin ());
        temp.assign(keys_.begin() + 3 * phase_length, keys_.end());
        std::move_backward(keys_.begin(), keys_.begin() + 3 * phase_length, keys_.end());
        std::copy(temp.begin(), temp.end(), keys_.begin());
        for (size_t j = 0; j < temp_read_dataset_length; j++) {
            uint64_t start =
                (segmentIndex[read_i_[j] - 1]) * segment_length;  // start position of key vector
            for (size_t k = 0; k < segment_length; k++) {
                read_keys_distribution_v3[j * segment_length + k] = keys_[start + k];
            }
        }
        INFO ("Phase 3 prepared");
        temp.clear ();
        // temp.insert (temp.end (), keys_.begin () + 3 * phase_length, keys_.end ());
        // // Shift phases 1, 2, 3 to the right
        // std::move_backward (keys_.begin (), keys_.begin () + 3 * phase_length, keys_.end ());
        // // Copy 4th phase to the beginning
        // std::copy (temp.begin (), temp.end (), keys_.begin ());
        temp.assign(keys_.begin() + 3 * phase_length, keys_.end());
        std::move_backward(keys_.begin(), keys_.begin() + 3 * phase_length, keys_.end());
        std::copy(temp.begin(), temp.end(), keys_.begin());

        for (size_t j = 0; j < temp_read_dataset_length; j++) {
            uint64_t start =
                (segmentIndex[read_i_[j] - 1]) * segment_length;  // start position of key vector
            for (size_t k = 0; k < segment_length; k++) {
                read_keys_distribution_v4[j * segment_length + k] = keys_[start + k];
            }
        }
        INFO ("Phase 4 prepared");
        
    }

    /**
     * We create a read workload with local sequential pattern in this function.
     * Local sequential also called segment here.
     * User is allowed to customize the length of the segment.
     **/
    void CreateReadWorkloadWithSegments (uint64_t read_dataset_length, uint64_t segment_length,
                                         double distrib_param, bool is_sparse_hot_spot) {
        uint64_t keyspace_length = count_;
        uint64_t temp_read_dataset_length = read_dataset_length / segment_length;
        uint64_t nSegments = keyspace_length / segment_length;
        std::vector<uint64_t> segmentIndex;
        segmentIndex.resize (nSegments);
        // segment i means that [i, i + segment_length)
        for (size_t idx = 0; idx < nSegments; idx++) {
            segmentIndex[idx] = idx;
        }

        if (is_sparse_hot_spot) {
            INFO ("Make the sparse hot spot");
            std::random_shuffle (segmentIndex.begin (), segmentIndex.end ());
        }

        std::default_random_engine generator;
        zipfian_int_distribution<int> distribution (1, nSegments, distrib_param);
        INFO ("[YCSB] Zipfian constant %2.2f KeySpace %lu nSegments %lu\n", distrib_param,
              keyspace_length, nSegments);
        read_keys_.resize (read_dataset_length);
        read_i_.resize (temp_read_dataset_length);

        std::unordered_map<uint64_t, uint64_t> umap;
        for (size_t j = 0; j < temp_read_dataset_length; j++) {
            read_i_[j] = distribution (generator);
            umap[segmentIndex[read_i_[j] - 1]] += 1;
        }

        for (size_t j = 0; j < temp_read_dataset_length; j++) {
            uint64_t start =
                (segmentIndex[read_i_[j] - 1]) * segment_length;  // start position of key vector
            for (size_t k = 0; k < segment_length; k++) {
                read_keys_[j * segment_length + k] = keys_[start + k];
            }
        }
        // ############################################################################################
        uint64_t count20 = 0, count40 = 0, count60 = 0, count80 = 0, count100 = 0;
        for (auto& item : umap) {
            if (item.first < nSegments * 0.2) count20 += item.second;
            if (item.first < nSegments * 0.4) count40 += item.second;
            if (item.first < nSegments * 0.6) count60 += item.second;
            if (item.first < nSegments * 0.8) count80 += item.second;
            if (item.first < nSegments * 1.0) count100 += item.second;
        }
        INFO ("[YCSB Data Distribution] 20 %2.3f 40 %2.3f 60 %2.3f 80 %2.3f 100 %2.3f\n",
              count20 * 1.0 / count100, count40 * 1.0 / count100, count60 * 1.0 / count100,
              count80 * 1.0 / count100, count100 * 1.0 / count100);
        INFO ("[YCSB] Unique number for generated workload %lu\n", umap.size ());
    }

    ~RandomKeyTrace () {}

    static std::string Name () { return TraceType::Name (); }

    void Randomize (void) {
        INFO ("randomize %lu keys\n", keys_.size ());
        auto starttime = std::chrono::system_clock::now ();
        std::random_shuffle (keys_.begin (), keys_.end ());
        std::random_shuffle (read_keys_.begin (), read_keys_.end ());
        auto duration = std::chrono::duration_cast<std::chrono::microseconds> (
            std::chrono::system_clock::now () - starttime);
        INFO ("randomize duration %f s.\n", duration.count () / 1000000.0);
    }

    class RangeIterator {
    public:
        RangeIterator (std::vector<size_t>* pkey_vec, size_t start, size_t end)
            : pkey_vec_ (pkey_vec), end_index_ (end), cur_index_ (start) {}

        inline bool Valid () { return (cur_index_ < end_index_); }

        inline size_t operator* () { return (*pkey_vec_)[cur_index_]; }

        inline size_t& Next () { return (*pkey_vec_)[cur_index_++]; }
        inline void RollBack () { cur_index_ = cur_index_ - 6000; }

        std::vector<size_t>* pkey_vec_;
        size_t end_index_;
        size_t cur_index_;
    };

    class Iterator {
    public:
        Iterator (std::vector<size_t>* pkey_vec, size_t start_index, size_t range)
            : pkey_vec_ (pkey_vec),
              range_ (range),
              end_index_ (start_index % range_),
              cur_index_ (start_index % range_),
              begin_ (true) {}

        Iterator () {}

        inline bool Valid () { return (begin_ || cur_index_ != end_index_); }

        inline size_t& Next () {
            begin_ = false;
            size_t index = cur_index_;
            cur_index_++;
            if (cur_index_ >= range_) {
                cur_index_ = 0;
            }
            return (*pkey_vec_)[index];
        }

        inline size_t operator* () { return (*pkey_vec_)[cur_index_]; }

        std::string Info () {
            char buffer[128];
            sprintf (buffer, "valid: %s, cur i: %lu, end_i: %lu, range: %lu",
                     Valid () ? "true" : "false", cur_index_, end_index_, range_);
            return buffer;
        }

        std::vector<size_t>* pkey_vec_;
        size_t range_;
        size_t end_index_;
        size_t cur_index_;
        bool begin_;
    };

    Iterator trace_at (size_t start_index, size_t range) {
        return Iterator (&keys_, start_index, range);
    }

    RangeIterator Begin (void) { return RangeIterator (&keys_, 0, keys_.size ()); }

    RangeIterator iterate_between (size_t start, size_t end) {
        return RangeIterator (&keys_, start, end);
    }

    RangeIterator read_iterate_between (size_t start, size_t end) {
        return RangeIterator (&read_keys_, start, end);
    }

    // For changing workload
    RangeIterator read_iterate_between_workload_1 (size_t start, size_t end) {
        return RangeIterator (&read_keys_distribution_v1, start, end);
    }

    RangeIterator read_iterate_between_workload_2 (size_t start, size_t end) {
        return RangeIterator (&read_keys_distribution_v2, start, end);
    }

    RangeIterator read_iterate_between_workload_3 (size_t start, size_t end) {
        return RangeIterator (&read_keys_distribution_v3, start, end);
    }

    RangeIterator read_iterate_between_workload_4 (size_t start, size_t end) {
        return RangeIterator (&read_keys_distribution_v4, start, end);
    }

    size_t count_;
    std::vector<size_t> keys_;
    std::vector<size_t> read_keys_;
    std::vector<size_t> read_i_;
    std::vector<size_t> read_keys_distribution_v1;
    std::vector<size_t> read_keys_distribution_v2;
    std::vector<size_t> read_keys_distribution_v3;
    std::vector<size_t> read_keys_distribution_v4;
};

enum YCSBOpType { kYCSB_Write, kYCSB_Read, kYCSB_Query, kYCSB_ReadModifyWrite };

inline uint32_t wyhash32 () {
    static thread_local uint32_t wyhash32_x = random ();
    wyhash32_x += 0x60bee2bee120fc15;
    uint64_t tmp;
    tmp = (uint64_t)wyhash32_x * 0xa3b195354a39b70d;
    uint32_t m1 = (tmp >> 32) ^ tmp;
    tmp = (uint64_t)m1 * 0x1b03738712fad5c9;
    uint32_t m2 = (tmp >> 32) ^ tmp;
    return m2;
}

class YCSBGenerator {
public:
    // Generate
    YCSBGenerator () {}

    inline YCSBOpType NextA () {
        // ycsba: 50% reads, 50% writes
        uint32_t rnd_num = wyhash32 ();

        if ((rnd_num & 0x1) == 0) {
            return kYCSB_Read;
        } else {
            return kYCSB_Write;
        }
    }

    inline YCSBOpType NextB () {
        // ycsbb: 95% reads, 5% writes
        // 51/1024 = 0.0498
        uint32_t rnd_num = wyhash32 ();

        if ((rnd_num & 1023) < 51) {
            return kYCSB_Write;
        } else {
            return kYCSB_Read;
        }
    }

    inline YCSBOpType NextC () { return kYCSB_Read; }

    inline YCSBOpType NextD () {
        // ycsbd: read latest inserted records
        uint32_t rnd_num = wyhash32 ();
        if ((rnd_num & 1023) < 51) {
            return kYCSB_Write;
        } else {
            return kYCSB_Read;
        }
    }

    inline YCSBOpType NextE () {
        // ycsbd: read latest inserted records
        uint32_t rnd_num = wyhash32 ();
        if ((rnd_num & 1023) < 51) {
            return kYCSB_Write;
        } else {
            return kYCSB_Read;
        }
    }

    inline YCSBOpType NextF () {
        // ycsba: 50% reads, 50% writes
        uint32_t rnd_num = wyhash32 ();

        if ((rnd_num & 0x1) == 0) {
            return kYCSB_Read;
        } else {
            return kYCSB_ReadModifyWrite;
        }
    }
};

