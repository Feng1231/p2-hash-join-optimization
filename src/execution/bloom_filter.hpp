#pragma once

#include "common/typedefs.hpp"
#include <vector>
#include <cstddef>
#include <functional>
#include <string>
#include <sstream>

namespace babydb {

class BloomFilter {
public:
    BloomFilter(size_t expected_entries) {
        size_t size = expected_entries * 10;
        size_t power = 1;
        while (power < size) power <<= 1;
        bits_.resize(power);
        size_ = power;
    }
    
    void Insert(const data_t& key) {
        std::string key_str = SerializeKey(key);
        for (size_t i = 0; i < NUM_HASHES; ++i) {
            size_t hash = HashString(key_str, i);
            bits_[hash % size_] = true;
        }
    }
    
    bool MightContain(const data_t& key) const {
        std::string key_str = SerializeKey(key);
        for (size_t i = 0; i < NUM_HASHES; ++i) {
            size_t hash = HashString(key_str, i);
            if (!bits_[hash % size_]) return false;
        }
        return true;
    }
    
    void Clear() {
        std::fill(bits_.begin(), bits_.end(), false);
    }
    
private:
    std::string SerializeKey(const data_t& key) const {
        std::ostringstream oss;
        oss << key;
        return oss.str();
    }
    
    size_t HashString(const std::string& str, size_t seed) const {
        // Double hashing technique
        std::hash<std::string> hasher;
        size_t hash1 = hasher(str);
        size_t hash2 = hasher(str + std::to_string(seed));
        return hash1 + seed * hash2;
    }
    
    static constexpr size_t NUM_HASHES = 3;
    std::vector<bool> bits_;
    size_t size_;
};

}