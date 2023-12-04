#ifndef ARTREE_COUNTER_X_H
#define ARTREE_COUNTER_X_H

#include <assert.h>
#include <stdint.h>

#include <atomic>
#include <numeric>
#include <string>
#include <unordered_map>
#include <vector>
namespace ART_OLC_X {
class GlobalMemoryInfo {
public:
    std::vector<uint64_t> allocatedMemory;
    std::vector<uint64_t> deallocatedMemory;

    GlobalMemoryInfo (uint64_t allocated, uint64_t deallocated) {
        allocatedMemory.resize (256);
        deallocatedMemory.resize (256);
        allocatedMemory[0] = allocated;
        deallocatedMemory[0] = deallocated;
    }

    void increaseByBytes (uint64_t tid, uint64_t nbytes) { allocatedMemory[tid & 255] += nbytes; }
    void decreaseByBytes (uint64_t tid, uint64_t nbytes) { deallocatedMemory[tid & 255] += nbytes; }
    uint64_t totalARTMemoryUsage () {
        uint64_t allocated = 0;
        for (auto i : allocatedMemory) allocated += i;
        uint64_t deallocated = 0;
        for (auto i : deallocatedMemory) deallocated += i;
        assert (allocated > deallocated);
        return allocated - deallocated;
    }
};

}  // namespace ART_OLC_X
#endif