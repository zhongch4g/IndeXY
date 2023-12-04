#pragma once

#include "utils.h"

using KeyLen = uint32_t;

class IndexKey {
    static constexpr uint32_t stackLen_ = 64;

    friend class SQLGenericKey;

    uint32_t len_ = 0;
    uint8_t* data_ = nullptr;
    uint8_t stack_key_[stackLen_];

public:
    IndexKey () = default;
    IndexKey (const IndexKey& key) = delete;
    IndexKey& operator= (const IndexKey& key) = delete;
    ~IndexKey ();

    // move ctor
    IndexKey (IndexKey&& key) {
        len_ = key.len_;
        if (len_ > stackLen_) {
            data_ = key.data_;
            key.data_ = nullptr;
        } else {
            memcpy (stack_key_, key.stack_key_, key.len_);
            data_ = stack_key_;
        }
    }

    // move assignment
    IndexKey& operator= (IndexKey&& key) {
        // free existing resource
        if (len_ > stackLen_) {
            delete[] data_;
        }

        if (key.len_ > key.stackLen_) {
            data_ = key.data_;
            key.data_ = nullptr;
        } else {
            memcpy (stack_key_, key.stack_key_, key.len_);
            data_ = stack_key_;
        }

        len_ = key.len_;
        return *this;
    }

    IndexKey (const std::string& s) {
        memcpy (stack_key_, s.data (), s.size ());
        data_ = stack_key_;
        len_ = s.size ();
    }

    // explicit conversion
    inline operator std::string () const { return std::string ((char*)data_, len_); }

    std::string ToString () {
        std::string tmp;
        for (uint32_t i = 0; i < len_; i++) {
            char buffer[8];
            sprintf (buffer, "%x", data_[i]);
            tmp += buffer;
        }
        tmp += '\0';
        return tmp;
    }

    inline bool operator== (const IndexKey& k) const {
        if (k.getKeyLen () != getKeyLen ()) {
            return false;
        }
        return std::memcmp (&k[0], data_, getKeyLen ()) == 0;
    }

    inline uint8_t& operator[] (std::size_t i) {
        assert (i < len_);
        return data_[i];
    }

    inline const uint8_t& operator[] (std::size_t i) const {
        assert (i < len_);
        return data_[i];
    }

    inline void set (const char bytes[], const std::size_t length);
    inline KeyLen getKeyLen () const { return len_; }
    inline void setKeyLen (KeyLen len);
    inline uint8_t* getData () const { return (uint8_t*)data_; }

    void CopyForSerialization (IndexKey& target) const {
        // only stack based working now for serialization
        assume (len_ <= stackLen_);

        memcpy (target.stack_key_, stack_key_, len_);
        target.data_ = const_cast<uint8_t*> (stack_key_);
        target.len_ = len_;
    }

    // this is a hack!
    void FixForDeserialize () {
        // only stack based working now for serialization
        assume (len_ <= stackLen_);
        data_ = const_cast<uint8_t*> (stack_key_);
    }
};

inline IndexKey::~IndexKey () {
    if (len_ > stackLen_) {
        delete[] data_;
        data_ = nullptr;
    }
}

inline void IndexKey::set (const char bytes[], const std::size_t newlen) {
    if (len_ > stackLen_) {
        delete[] data_;
    }
    if (newlen <= stackLen_) {
        memcpy (stack_key_, bytes, newlen);
        data_ = stack_key_;
    } else {
        data_ = new uint8_t[newlen];
        memcpy (data_, bytes, newlen);
    }
    len_ = newlen;
}

inline void IndexKey::setKeyLen (KeyLen newLen) {
    if (len_ == newLen) return;
    if (len_ > stackLen_) {
        delete[] data_;
    }
    len_ = newLen;
    if (len_ > stackLen_) {
        data_ = new uint8_t[len_];
    } else {
        data_ = stack_key_;
    }
}
