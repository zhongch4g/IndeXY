#include "Policy.h"

#include <algorithm>
#include <cmath>
#include <iostream>
#include <numeric>

#include "artree-shared-headers/Epoche.cpp"
#include "artree-shared-headers/xoshiro256.h"

namespace ART_OLC_X {

double CalculateSD (std::vector<double>& nums) {
    if (nums.size () == 0) return 0;
    double sum = std::accumulate (nums.begin (), nums.end (), 0);
    double mean = sum / nums.size ();
    if (mean == 0.0) return 0;

    double deviation = 0;
    for (double n : nums) {
        deviation += (n - mean) * (n - mean);
    }
    deviation /= nums.size ();
    deviation = sqrt (deviation);

    // normalize deviation by deviding mean
    return deviation / mean;
}

UnloadPolicy* UnloadPolicyFactory (UnloadPolicyType type) {
    switch (type) {
        case kUnloadDefaultType: {
            return new Policy3;
        }
        case kClockBasedType: {
            return new Policy3;
        }

        default: {
            return new Policy3;
        }
    }
}

std::vector<candidate_t> Policy3::Expand (std::vector<candidate_t>& q1, size_t dumpSize,
                                          ThreadInfo& epocheInfo,
                                          std::vector<candidate_t*>& publicList,
                                          uint64_t& lengthOfpublicList) {
    EpocheGuard epocheGuard (epocheInfo);
    // Expand
    uint64_t maxPublistSize = publicList.size ();

    std::vector<candidate_t> Childs;
    std::vector<std::string> nodeTypes;
    uint64_t idx = 0;
    // traverse the q1
    for (int q1ID = 0; q1ID < q1.size (); q1ID++) {
        N* curNode = q1[q1ID].node;
        if (N::isLeaf (curNode)) {
            continue;
        }
        std::string new_prefix =
            q1[q1ID].prefix +
            (curNode->hasPrefix ()
                 ? std::string ((char*)curNode->getPrefix (),
                                std::min (curNode->getPrefixLength (), maxStoredPrefixLength))
                 : "");
        // check the type of the node
        switch (curNode->getType ()) {
            case NTypes::N4: {
                N4* n = static_cast<N4*> (curNode);

                for (uint32_t i = 0; i < n->getCount (); i++) {
                    N* child = n->children[i];
                    if (N::isLeaf (child) || (child == nullptr)) continue;
                    new_prefix.push_back (n->keys[i]);

                    if (dumpSize == 9999999) {
                        // if (Random ::next () & 0x1) continue;
                        publicList[idx]->density = 1.0;
                        publicList[idx]->node = child;
                        publicList[idx]->prefix = new_prefix;
                        if (child) {
                            child->candidate = publicList[idx++];
                        }
                        if (idx == maxPublistSize) {
                            lengthOfpublicList = idx;
                            return Childs;
                        }
                    } else {
                        Childs.push_back (candidate_t{1.0, child, new_prefix});
                    }
                    nodeTypes.push_back ("N4");
                    new_prefix.pop_back ();
                }
                nodeTypes.push_back ("|");
                break;
            }
            case NTypes::N16: {
                N16* n = static_cast<N16*> (curNode);

                for (uint32_t i = 0; i < n->getCount (); i++) {
                    N* child = n->children[i];
                    if (N::isLeaf (child) || (child == nullptr)) continue;
                    new_prefix.push_back (N16::flipSign (n->keys[i]));
                    if (dumpSize == 9999999) {
                        // if (Random ::next () & 0x1) continue;
                        publicList[idx]->density = 1.0;
                        publicList[idx]->node = child;
                        publicList[idx]->prefix = new_prefix;
                        if (child) {
                            child->candidate = publicList[idx++];
                        }
                        if (idx == maxPublistSize) {
                            lengthOfpublicList = idx;
                            return Childs;
                        }
                    } else {
                        Childs.push_back (candidate_t{1.0, child, new_prefix});
                    }
                    nodeTypes.push_back ("N16");
                    new_prefix.pop_back ();
                }
                nodeTypes.push_back ("|");
                break;
            }
            case NTypes::N48: {
                N48* n = static_cast<N48*> (curNode);

                for (uint32_t i = 0; i < 256; i++) {
                    if (n->childIndex[i] != 48) {
                        auto child = n->children[n->childIndex[i]];
                        if (N::isLeaf (child) || (child == nullptr)) continue;
                        new_prefix.push_back (char (i));
                        if (dumpSize == 9999999) {
                            // if (Random ::next () & 0x1) continue;
                            publicList[idx]->density = 1.0;
                            publicList[idx]->node = child;
                            publicList[idx]->prefix = new_prefix;
                            if (child) {
                                child->candidate = publicList[idx++];
                            }
                            if (idx == maxPublistSize) {
                                lengthOfpublicList = idx;
                                return Childs;
                            }
                        } else {
                            Childs.push_back (candidate_t{1.0, child, new_prefix});
                        }
                        nodeTypes.push_back ("N48");
                        new_prefix.pop_back ();
                    }
                }
                nodeTypes.push_back ("|");
                break;
            }
            case NTypes::N256: {
                N256* n = static_cast<N256*> (curNode);

                for (uint32_t i = 0; i < 256; i++) {
                    if (n->children[i] != nullptr) {
                        auto child = n->children[i];
                        if (N::isLeaf (child) || (child == nullptr)) continue;
                        new_prefix.push_back (char (i));
                        if (dumpSize == 9999999) {
                            // if (Random ::next () & 0x1) continue;
                            publicList[idx]->density = 1.0;
                            publicList[idx]->node = child;
                            publicList[idx]->prefix = new_prefix;
                            if (child) {
                                child->candidate = publicList[idx++];
                            }
                            if (idx == maxPublistSize) {
                                lengthOfpublicList = idx;
                                return Childs;
                            }
                        } else {
                            Childs.push_back (candidate_t{1.0, child, new_prefix});
                        }
                        nodeTypes.push_back ("N256");
                        new_prefix.pop_back ();
                    }
                }
                nodeTypes.push_back ("|");
                break;
            }
            default: {
                assert (false);
                __builtin_unreachable ();
            }
        }
    }
    lengthOfpublicList = idx;
    // if (dumpSize != 9999999) {
    // for (int i = 0; i < q1.size (); i++) {
    // printf ("%s:%p ", nodeTypes[i].c_str (), (void*)q1[i].node);
    // }
    // printf ("\n");
    // printf ("The size of the childs %lu\n", Childs.size ());
    // } else {
    // for (int i = 0; i < q1.size (); i++) {
    // printf ("%s:%p ", nodeTypes[i].c_str (), (void*)q1[i].node);
    // }
    // printf ("\n");
    // for (int i = 0; i < q1.size (); i++) {
    //     q1[i].node->hybridBitmap.toString ();
    //     printf (" ");
    // }
    // printf ("The size of the childs %lu\n", lengthOfpublicList);
    // }
    return Childs;
}

std::vector<candidate_t> Policy3::GetVictims (N* root, size_t dumpSize, ThreadInfo& epocheInfo,
                                              std::vector<candidate_t*>& publicList,
                                              uint64_t& lengthOfpublicList, uint8_t urlevel) {
    std::vector<candidate_t> cqueue;
    std::vector<candidate_t> afterExpand;
    std::string info = "";
    int64_t target_size = dumpSize;
    cqueue.push_back (candidate_t{0, root, Key ("")});
    // split the nodes
    for (uint8_t i = 0; i < urlevel + 1; i++) {
        if (i == urlevel) {
            target_size = 9999999;
        }
        afterExpand = Expand (cqueue, target_size, epocheInfo, publicList, lengthOfpublicList);
        cqueue = afterExpand;
        info += std::to_string (cqueue.size ());
        info += " -> ";
    }
    info += std::to_string (lengthOfpublicList);
    INFO ("[POLICY] Level %lu | %s\n", urlevel, info.c_str ());
    return cqueue;
}

}  // namespace ART_OLC_X