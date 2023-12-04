#pragma once

#include <inttypes.h>
#include <sched.h>

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

/**
 *	compiler characteristics
 *  ------------------------
 */
#define likely(x) (__builtin_expect (!!(x), 1))
#define unlikely(x) (__builtin_expect (!!(x), 0))
#define must_check __attribute__ ((warn_unused_result))
#define never_inline __attribute__ ((noinline))

#define per_row_perf __attribute__ ((hot))

// string macros
//
#define CONCAT_IMPL(x, y) x##y
#define MACRO_CONCAT(x, y) CONCAT_IMPL (x, y)
#define MAKE_UNIQUENAME(var) MACRO_CONCAT (var, __COUNTER__)

// function arguments marker for readability
//
#define __OUT    // output argument only, its initial content not used
#define __INOUT  // output and input argument, its initial content used

// Uses this macro when you believe code flow won't reach there. Without 'assert (false)' the
// program may fail at strange places and causes confusion
//
#define mark_not_reachable()      \
    do {                          \
        assert (false);           \
        __builtin_unreachable (); \
    } while (0)

// Creates a compiler memory barrier enforcing no memory accesses reordering. Compiler also treat it
// as a function call so enforce memory reload after.
//
#define compiler_memory_barrier() asm volatile ("" ::: "memory")

/**
 *	debug support
 *  ------------------------
 */
// assume is a shortcut of "we actually should check the condition here"
#ifndef assert
#define assert assert
#endif
#define assume assert

#ifndef NDEBUG
#define DBG_ONLY(x) x
#define RTL_ONLY(x)
#define CONFIG_VALUE(dbg, rtl) (dbg)
#else
#define DBG_ONLY(x)
#define RTL_ONLY(x) x
#define CONFIG_VALUE(dbg, rtl) (rtl)
#endif

//  Usual C++ structure symbols are hidden by `-fvisibility=hidden` linker option.
//  To make it available to source code generation, make its visibility `default`.
//
#define CODEGEN_VISIBILITY __attribute__ ((visibility ("default")))

// use this assert if you need it at release time
CODEGEN_VISIBILITY void rtl_assert (const char* expression, const char* file, const char* func,
                                    unsigned line);
#define RTL_ASSERT(expression)        \
    (void)(likely (!!(expression)) || \
           (rtl_assert ((#expression), __FILE__, __PRETTY_FUNCTION__, __LINE__), 0))

#define DBG_SCHED_YIELD(N)            \
    DBG_ONLY (do {                    \
        int n = (N);                  \
        for (int i = 0; i < n; i++) { \
            sched_yield ();           \
        }                             \
    } while (0))

// common hash function macro, cyclic shift to avoid same value
#define SHIFT_HASH(hash) ((hash >> 59) | (((hash << 5) - 1)))
#define SHIFT_HASH32(hash) ((hash >> 27) | (((hash << 5) - 1)))

/**
 *	miscellaneous items
 *  ------------------------
 */
constexpr int CacheLineBytes = 64;

// SourceInsight 4 is confused with namespace alias. Use an macro to replace it so it can work. We
// can remove this when it fixes the issue.
//
#define NameSpaceAlias namespace

// magic to control lambda callback loops
enum LoopControl : int {
    continue_loop = 0,   // continue the loop
    ignore_subtree = 1,  // continue the loop, but do not go deeper in subtree (PreOrder only)
    break_loop = 2,  // break the loop, return immediately. This is mostly used by "exists" check.
};
