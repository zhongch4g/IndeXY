#ifndef ART_POLICY_X_H
#define ART_POLICY_X_H

#include <string>
#include <unordered_map>
#include <vector>

#include "N.h"
#include "logger.h"

namespace ART_OLC_X {

double CalculateSD (std::vector<double>& nums);

void recordUnloadedStatistic (Key key, uint64_t level);

enum UnloadPolicyType {
    kUnloadDefaultType = 0,
    kUnload4thLevelType = 1,
    kClockBasedType = 2,
    kUnloadTypeEnd
};

class UnloadPolicy {
public:
    std::vector<candidate_t> clockArray;
    virtual std::vector<candidate_t> GetVictims (N* root, size_t dumpSize, ThreadInfo& epocheInfo,
                                                 std::vector<candidate_t*>& publicList,
                                                 uint64_t& lengthOfpublicList,
                                                 uint8_t urlevel = 3) = 0;
    virtual ~UnloadPolicy () = default;
};

class Policy3 : public UnloadPolicy {
public:
    std::vector<candidate_t> Expand (std::vector<candidate_t>& q1, size_t dumpSize,
                                     ThreadInfo& epocheInfo, std::vector<candidate_t*>& publicList,
                                     uint64_t& lengthOfpublicList);
    std::vector<candidate_t> GetVictims (N* root, size_t dumpSize, ThreadInfo& epocheInfo,
                                         std::vector<candidate_t*>& publicList,
                                         uint64_t& lengthOfpublicList, uint8_t urlevel) override;
    void releaseThread ();
    void unloadThread ();
};

UnloadPolicy* UnloadPolicyFactory (UnloadPolicyType type);

}  // namespace ART_OLC_X

#endif