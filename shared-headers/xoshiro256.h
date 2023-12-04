/*  Written in 2018 by David Blackman and Sebastiano Vigna (vigna@acm.org)

To the extent possible under law, the author has dedicated all copyright
and related and neighboring rights to this software to the public domain
worldwide. This software is distributed without any warranty.

See <http://creativecommons.org/publicdomain/zero/1.0/>. */

#ifndef XOSHIRO256_H
#define XOSHIRO256_H

#include <stdint.h>

#include <atomic>
#include <random>

/* This is xoshiro256+ 1.0, our best and fastest generator for floating-point
   numbers. We suggest to use its upper bits for floating-point
   generation, as it is slightly faster than xoshiro256++/xoshiro256**. It
   passes all tests we are aware of except for the lowest three bits,
   which might fail linearity tests (and just those), so if low linear
   complexity is not considered an issue (as it is usually the case) it
   can be used to generate 64-bit outputs, too.

   We suggest to use a sign test to extract a random Boolean value, and
   right shifts to extract subsets of bits.

   The state must be seeded so that it is not everywhere zero. If you have
   a 64-bit seed, we suggest to seed a splitmix64 generator and use its
   output to fill s. */

namespace ART_OLC_X {

class RndGen {
public:
    RndGen (int seed) : seed_ (seed) {}

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

    int seed_;
};

class Random {
public:
    static inline uint64_t rotl (const uint64_t x, int k) { return (x << k) | (x >> (64 - k)); }
    static inline thread_local uint64_t mSeed[4];
    static inline thread_local bool mInitialized = false;
    static inline std::atomic<uint64_t> rids{1};

    static void Initialize (uint64_t id) {
        if (!mInitialized) {
            mInitialized = true;
            RndGen rndGen (id * 1234567);

            for (int i = 0; i < 4; i++) {
                mSeed[i] = rndGen.Random ();
            }
        }
    }

    static uint64_t next (void) {
        if (__glibc_unlikely (!mInitialized)) {
            uint64_t mid = rids.fetch_add (1);
            mInitialized = true;
            RndGen rndGen (mid * 1234567);

            for (int i = 0; i < 4; i++) {
                mSeed[i] = rndGen.Random ();
            }
        }

        const uint64_t result = mSeed[0] + mSeed[3];

        const uint64_t t = mSeed[1] << 17;

        mSeed[2] ^= mSeed[0];
        mSeed[3] ^= mSeed[1];
        mSeed[1] ^= mSeed[2];
        mSeed[0] ^= mSeed[3];

        mSeed[2] ^= t;

        mSeed[3] = rotl (mSeed[3], 45);

        return result;
    }

    static uint64_t next48 (void) {
        if (__glibc_unlikely (!mInitialized)) {
            uint64_t mid = rids.fetch_add (1);
            mInitialized = true;
            RndGen rndGen (mid * 1234567);

            for (int i = 0; i < 4; i++) {
                mSeed[i] = rndGen.Random ();
            }
        }

        const uint64_t result = mSeed[0] + mSeed[3];

        const uint64_t t = mSeed[1] << 17;

        mSeed[2] ^= mSeed[0];
        mSeed[3] ^= mSeed[1];
        mSeed[1] ^= mSeed[2];
        mSeed[0] ^= mSeed[3];

        mSeed[2] ^= t;

        mSeed[3] = rotl (mSeed[3], 45);

        return result & 0x0000ffffffffffff;
    }
};

}  // namespace ART_OLC_X

#endif