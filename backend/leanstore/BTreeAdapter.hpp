#include "Units.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
unsigned fold (uint8_t* writer, const s32& x) {
    *reinterpret_cast<u32*> (writer) = __builtin_bswap32 (x ^ (1ul << 31));
    return sizeof (x);
}

unsigned fold (uint8_t* writer, const s64& x) {
    *reinterpret_cast<u64*> (writer) = __builtin_bswap64 (x ^ (1ull << 63));
    return sizeof (x);
}

unsigned fold (uint8_t* writer, const u64& x) {
    *reinterpret_cast<u64*> (writer) = __builtin_bswap64 (x);
    return sizeof (x);
}

unsigned fold (uint8_t* writer, const u32& x) {
    *reinterpret_cast<u32*> (writer) = __builtin_bswap32 (x);
    return sizeof (x);
}
// -------------------------------------------------------------------------------------
template <typename Key, typename Payload>
struct BTreeInterface {
    virtual bool lookup (Key k, Payload& v) = 0;
    virtual void insert (Key k, Payload& v) = 0;
    virtual void update (Key k, Payload& v) = 0;
    virtual void scan (Key k) = 0;
    virtual void ToStats () = 0;
};
// -------------------------------------------------------------------------------------
using OP_RESULT = leanstore::storage::btree::OP_RESULT;
template <typename Key, typename Payload>
struct BTreeVSAdapter : BTreeInterface<Key, Payload> {
    leanstore::storage::btree::BTreeInterface& btree;

    BTreeVSAdapter (leanstore::storage::btree::BTreeInterface& btree) : btree (btree) {}

    bool lookup (Key k, Payload& v) override {
        u8 key_bytes[sizeof (Key)];
        return btree.lookup (key_bytes, fold (key_bytes, k),
                             [&] (const u8* payload, u16 payload_length) {
                                 memcpy (&v, payload, payload_length);
                             }) == OP_RESULT::OK;
    }
    void insert (Key k, Payload& v) override {
        u8 key_bytes[sizeof (Key)];
        btree.insert (key_bytes, fold (key_bytes, k), reinterpret_cast<u8*> (&v), sizeof (v));
    }
    void update (Key k, Payload& v) override {
        u8 key_bytes[sizeof (Key)];
        btree.updateSameSize (
            key_bytes, fold (key_bytes, k),
            [&] (u8* payload, u16 payload_length) { memcpy (payload, &v, payload_length); });
    }
    void ToStats () override { btree.ToStats (); }

    void scan (Key k) override {
        u8 key_bytes[sizeof (Key)];
        uint64_t i = 0;
        uint64_t scan_length = 1 + leanstore::utils::RandomGenerator::getRand (0, 100);
        btree.scanAsc (
            key_bytes, fold (key_bytes, k),
            [&] (const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
                if (i < scan_length) {
                    i++;
                    return true;
                }

                return false;
            },
            [&] {});
    }
};
// -------------------------------------------------------------------------------------
template <u64 size>
struct BytesPayload {
    u8 value[size];
    BytesPayload () {}
    bool operator== (BytesPayload& other) {
        return (std::memcmp (value, other.value, sizeof (value)) == 0);
    }
    bool operator!= (BytesPayload& other) { return !(operator== (other)); }
    BytesPayload (const BytesPayload& other) { std::memcpy (value, other.value, sizeof (value)); }
    BytesPayload& operator= (const BytesPayload& other) {
        std::memcpy (value, other.value, sizeof (value));
        return *this;
    }
};
}  // namespace leanstore
