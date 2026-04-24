#pragma once

#include <vector>
#include <functional>
#include <bitset>
#include <cmath>

namespace babydb {

class BloomFilter {
public:
    BloomFilter(size_t size, size_t num_hashes)
        : size_(size), num_hashes_(num_hashes), bit_array_(size) {}

    void Add(const std::string &key) {
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t hash = Hash(key, i);
            bit_array_[hash % size_] = true;
        }
    }

    bool PossiblyContains(const std::string &key) const {
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t hash = Hash(key, i);
            if (!bit_array_[hash % size_]) {
                return false;
            }
        }
        return true;
    }

private:
    size_t Hash(const std::string &key, size_t seed) const {
        return std::hash<std::string>{}(key) ^ (seed * 0x9e3779b9);
    }

    size_t size_;
    size_t num_hashes_;
    std::vector<bool> bit_array_;
};

} // namespace babydb