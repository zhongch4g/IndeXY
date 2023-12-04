#ifndef KV_TRACE_H_
#define KV_TRACE_H_

#include <math.h>
#include <stdint.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>
namespace util {

#ifndef typeof
#define typeof __typeof__
#endif

enum GeneratorType {
    GEN_CONSTANT = 0,  // always a constant number
    GEN_COUNTER,       // +1 each fetch
    GEN_DISCRETE,      // gen within a set of values with its weight as probability
    GEN_EXPONENTIAL,   // exponential
    GEN_FILE,          // string from lines in file
    GEN_HISTOGRAM,     // histogram
    GEN_HOTSET,        // hot/cold set
    GEN_ZIPFIAN,       // Zipfian, 0 is the most popular.
    GEN_XZIPFIAN,      // ScrambledZipfian. scatters the "popular" items across the itemspace.
    GEN_LATEST,        // Generate a popularity distribution of items, skewed to favor recent items.
    GEN_UNIFORM,       // Uniformly distributed in an interval [a,b]
    GEN_NORMAL,        // Normal distribution
};

struct GenInfo_Constant {
    uint64_t constant;
};

struct GenInfo_Counter {
    uint64_t counter;
};

struct Pair64 {
    uint64_t a;
    uint64_t b;
};

struct GenInfo_Discrete {
    uint64_t nr_values;
    struct Pair64* pairs;
};

struct GenInfo_Exponential {
    double gamma;
    uint64_t max;
};

struct GenInfo_File {
    FILE* fin;
};

struct GenInfo_Histogram {
    uint64_t block_size;
    uint64_t area;
    uint64_t weighted_area;
    double main_size;
    uint64_t* buckets;
};

struct GenInfo_HotSet {
    uint64_t lower_bound;
    uint64_t upper_bound;
    uint64_t hot_interval;
    uint64_t cold_interval;
    double hot_set_fraction;
    double hot_op_fraction;
};

#define ZIPFIAN_CONSTANT ((1.05))
struct GenInfo_Zipfian {
    uint64_t nr_items;
    uint64_t base;
    double zipfian_constant;
    double theta;
    double zeta2theta;
    double alpha;
    double zetan;
    double eta;
    uint64_t countforzeta;
    uint64_t min;
    uint64_t max;
};

struct GenInfo_Latest {
    struct GenInfo_Zipfian zipfion;
    uint64_t max;
};

struct GenInfo_Uniform {
    uint64_t min;
    uint64_t max;
    double interval;
};

struct GenInfo_Normal {
    uint64_t mean;
    uint64_t stddev;
    uint64_t min;
    uint64_t max;
};

struct GenInfo {
    uint64_t (*next) (struct GenInfo* const);
    enum GeneratorType type;
    union {
        struct GenInfo_Constant constant;
        struct GenInfo_Counter counter;
        struct GenInfo_Discrete discrete;
        struct GenInfo_Exponential exponential;
        struct GenInfo_File file;
        struct GenInfo_Histogram histogram;
        struct GenInfo_HotSet hotset;
        struct GenInfo_Zipfian zipfian;
        struct GenInfo_Latest latest;
        struct GenInfo_Uniform uniform;
        struct GenInfo_Normal normal;
    } gen;
    std::string get_type ();
};

const uint64_t kRAND64_MAX = ((((uint64_t)RAND_MAX) << 31) + ((uint64_t)RAND_MAX));
const double kRAND64_MAX_D = ((double)(kRAND64_MAX));
// const uint64_t kRANDOM_RANGE = UINT64_C (2000000000000);
const uint64_t kRANDOM_RANGE = UINT64_C (281474976710655);  // 48 bits

const uint64_t kYCSB_SEED = 1729;
const uint64_t kYCSB_LATEST_SEED = 1089;

class Trace {
public:
    Trace (int seed) : seed_ (seed), init_ (seed), gi_ (nullptr) {}

    virtual ~Trace () {
        if (gi_ != nullptr) delete gi_;
    }
    virtual uint64_t Next () = 0;

    uint32_t Random () {
        static const uint32_t M = 2147483647L;  // 2^31-1
        static const uint64_t A = 16807;        // bits 14, 8, 7, 5, 2, 1, 0
        // We are computing
        //       seed_ = (seed_ * A) % M,    where M = 2^31-1
        //
        // seed_ must not be zero or M, or else all subsequent computed values
        // will be zero or M respectively.  For all other values, seed_ will end
        // up cycling through every number in [1,M-1]
        uint64_t product = seed_ * A;

        // Compute (product % M) using the fact that ((x << 31) % M) == x.
        seed_ = static_cast<uint32_t> ((product >> 31) + (product & M));
        // The first reduction may overflow by 1 bit, so we may need to
        // repeat.  mod == M is not possible; using > allows the faster
        // sign-bit-based test.
        if ((uint32_t)seed_ > M) {
            seed_ -= M;
        }
        return seed_;
    }

    uint64_t Random64 () {
        // 62 bit random value;
        const uint64_t rand64 = (((uint64_t)random ()) << 31) + ((uint64_t)random ());
        return rand64;
    }

    double RandomDouble () {
        // random between 0.0 - 1.0
        const double r = (double)Random64 ();
        const double rd = r / kRAND64_MAX_D;
        return rd;
    }

    int seed_;
    int init_;
    GenInfo* gi_;
};

class TraceUniform : public Trace {
public:
    explicit TraceUniform (uint64_t minimum = 1, uint64_t maximum = kRANDOM_RANGE);
    ~TraceUniform () {}
    uint64_t Next () override;
    static std::string Name () { return "Uniform"; }
};

// uniform
TraceUniform::TraceUniform (uint64_t minimum, uint64_t maximum) : Trace (2333) {
    gi_ = new GenInfo ();
    gi_->gen.uniform.min = minimum;
    gi_->gen.uniform.max = maximum;
    gi_->gen.uniform.interval = (double)(maximum - minimum);
    gi_->type = GEN_UNIFORM;
}

uint64_t TraceUniform::Next () {
    const uint64_t off = (uint64_t)(RandomDouble () * gi_->gen.uniform.interval);
    return gi_->gen.uniform.min + off;
}

class TraceExponential : public Trace {
public:
#define FNV_OFFSET_BASIS_64 ((UINT64_C (0xCBF29CE484222325)))
#define FNV_PRIME_64 ((UINT64_C (1099511628211)))
    explicit TraceExponential (double range_min = 1, double range = kRANDOM_RANGE,
                               const double percentile = 90);
    ~TraceExponential () {}
    uint64_t Next () override;
    static std::string Name () { return "Exp"; }

private:
    uint64_t range_;
};

// exponential
TraceExponential::TraceExponential (double range_min, double range, const double percentile)
    : Trace (667), range_ (range) {
    range = range * 0.15;
    gi_ = new GenInfo ();
    gi_->gen.exponential.gamma = -log (1.0 - (percentile / 100.0)) / range;
    (void)range_min;
    gi_->type = GEN_EXPONENTIAL;
}

uint64_t TraceExponential::Next () {
    uint64_t d = (uint64_t)(-log (RandomDouble ()) / gi_->gen.exponential.gamma) % range_;
    return d;
}

class TraceNormal : public Trace {
public:
    explicit TraceNormal (uint64_t minimum = 1, uint64_t maximum = kRANDOM_RANGE,
                          double offset = 21.0);
    ~TraceNormal () {}
    uint64_t Next () override;
    static std::string Name () { return "Normal"; }

    std::default_random_engine generator;
    double offset_;

    // the smaller the std devistion is, the skewer of the the distribution
    std::normal_distribution<double> distribution;
};

// ===================================================
// = Normal Distribution Reference                   =
// = https://www.johndcook.com/blog/cpp_phi_inverse/ =
// ===================================================
TraceNormal::TraceNormal (uint64_t minimum, uint64_t maximum, double offset)
    : Trace (997), distribution (0, 0.2 /* std deviation */) {
    gi_ = new GenInfo ();
    gi_->gen.normal.min = minimum;
    gi_->gen.normal.max = maximum;
    gi_->gen.normal.mean = (maximum + minimum) / 2;
    gi_->gen.normal.stddev = (maximum - minimum) / 4;
    gi_->type = GEN_NORMAL;
    generator.seed (1234567);
    offset_ = offset;
}

uint64_t TraceNormal::Next () {
    // make sure rnd in (0,6)
    // double rnd = distribution (generator) + 3.0;
    double rnd = distribution (generator) + offset_;
    while (rnd < 0 || rnd > 6) {
        rnd = distribution (generator) + offset_;
    }

    uint64_t res = rnd * ((gi_->gen.normal.max - gi_->gen.normal.min) / 6.0);
    return res;
}

class TraceZipfian : public Trace {
public:
    explicit TraceZipfian (int seed, uint64_t minimum = 0,
                           uint64_t maximum = UINT64_C (0xc0000000000),
                           double zipfian_constant = 0.85);
    ~TraceZipfian () {}
    uint64_t Next () override;
    uint64_t NextRaw ();
    double Zeta (const uint64_t n, const double theta);
    double ZetaRange (const uint64_t start, const uint64_t count, const double theta);
    uint64_t FNVHash64 (const uint64_t value);

private:
    uint64_t zetalist_u64[17] = {
        0,
        UINT64_C (0x4040437dd948c1d9),
        UINT64_C (0x4040b8f8009bce85),
        UINT64_C (0x4040fe1121e564d6),
        UINT64_C (0x40412f435698cdf5),
        UINT64_C (0x404155852507a510),
        UINT64_C (0x404174d7818477a7),
        UINT64_C (0x40418f5e593bd5a9),
        UINT64_C (0x4041a6614fb930fd),
        UINT64_C (0x4041bab40ad5ec98),
        UINT64_C (0x4041cce73d363e24),
        UINT64_C (0x4041dd6239ebabc3),
        UINT64_C (0x4041ec715f5c47be),
        UINT64_C (0x4041fa4eba083897),
        UINT64_C (0x4042072772fe12bd),
        UINT64_C (0x4042131f5e380b72),
        UINT64_C (0x40421e53630da013),
    };

    double* zetalist_double = (double*)zetalist_u64;
    uint64_t zetalist_step = UINT64_C (0x10000000000);
    uint64_t zetalist_count = 16;
    // double zetalist_theta = 0.99;
    uint64_t range_;
};

// zipfian
TraceZipfian::TraceZipfian (int seed, uint64_t minimum, uint64_t maximum, double zipfian_constant)
    : Trace (seed), range_ (maximum) {
    gi_ = new GenInfo ();
    struct GenInfo_Zipfian* const gz = &(gi_->gen.zipfian);

    const uint64_t items = maximum - minimum + 1;
    gz->nr_items = items;
    gz->base = minimum;
    gz->zipfian_constant = zipfian_constant;
    gz->theta = zipfian_constant;
    gz->zeta2theta = Zeta (2, zipfian_constant);
    gz->alpha = 1.0 / (1.0 - zipfian_constant);
    double zetan = Zeta (items, zipfian_constant);
    gz->zetan = zetan;
    gz->eta = (1.0 - std::pow (2.0 / (double)items, 1.0 - zipfian_constant)) /
              (1.0 - (gz->zeta2theta / zetan));
    gz->countforzeta = items;
    gz->min = minimum;
    gz->max = maximum;

    gi_->type = GEN_ZIPFIAN;
}

double TraceZipfian::Zeta (const uint64_t n, const double theta) {
    // assert(theta == zetalist_theta);
    const uint64_t zlid0 = n / zetalist_step;
    const uint64_t zlid = (zlid0 > zetalist_count) ? zetalist_count : zlid0;
    const double sum0 = zetalist_double[zlid];
    const uint64_t start = zlid * zetalist_step;
    const uint64_t count = n - start;
    assert (n > start);
    const double sum1 = ZetaRange (start, count, theta);
    return sum0 + sum1;
}

double TraceZipfian::ZetaRange (const uint64_t start, const uint64_t count, const double theta) {
    double sum = 0.0;
    if (count > 0x10000000) {
        fprintf (stderr, "zeta_range would take a long time... kill me or wait\n");
    }
    for (uint64_t i = 0; i < count; i++) {
        sum += (1.0 / pow ((double)(start + i + 1), theta));
    }
    return sum;
}

uint64_t TraceZipfian::FNVHash64 (const uint64_t value) {
    uint64_t hashval = FNV_OFFSET_BASIS_64;
    uint64_t val = value;
    for (int i = 0; i < 8; i++) {
        const uint64_t octet = val & 0x00ff;
        val = val >> 8;
        // FNV-1a
        hashval = (hashval ^ octet) * FNV_PRIME_64;
    }
    return hashval;
}

uint64_t TraceZipfian::NextRaw () {
    // simplified: no increamental update
    const GenInfo_Zipfian* gz = &(gi_->gen.zipfian);
    const double u = RandomDouble ();
    const double uz = u * gz->zetan;
    if (uz < 1.0) {
        return gz->base + 0lu;
    } else if (uz < (1.0 + pow (0.5, gz->theta))) {
        return gz->base + 1lu;
    }
    const double x = ((double)gz->nr_items) * pow (gz->eta * (u - 1.0) + 1.0, gz->alpha);
    const uint64_t ret = gz->base + (uint64_t)x;
    return ret;
}

uint64_t TraceZipfian::Next () {
    // ScrambledZipfian. scatters the "popular" items across the itemspace.
    const uint64_t z = NextRaw ();
    const uint64_t xz = gi_->gen.zipfian.min + (FNVHash64 (z) % gi_->gen.zipfian.nr_items);
    return xz % range_;
}

}  // namespace util
#endif