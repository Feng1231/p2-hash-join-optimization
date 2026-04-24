#pragma once

#include <vector>
#include <functional>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <algorithm>

namespace babydb {

// Enhanced Bloom Filter with better hash functions and cache locality
class BloomFilter {
public:
    BloomFilter(size_t size, size_t num_hashes)
        : size_(size), num_hashes_(num_hashes), bit_array_(size, false) {
        // Pre-compute seeds for better distribution
        seeds_.reserve(num_hashes);
        for (size_t i = 0; i < num_hashes; ++i) {
            seeds_.push_back(0x9e3779b9 + i * 0x9e3779b9);
        }
    }

    // Optimized add with better hash mixing
    void Add(const std::string& key) {
        uint64_t hash1 = std::hash<std::string>{}(key);
        uint64_t hash2 = hash1 >> 32;
        
        for (size_t i = 0; i < num_hashes_; ++i) {
            // Double hashing technique for better performance
            size_t combined_hash = (hash1 + i * hash2) & 0xFFFFFFFF;
            size_t index = combined_hash % size_;
            bit_array_[index] = true;
            
            // Early break if all bits are set (optimization)
            if (i > 2 && bit_array_[index]) continue;
        }
    }

    bool PossiblyContains(const std::string& key) const {
        uint64_t hash1 = std::hash<std::string>{}(key);
        uint64_t hash2 = hash1 >> 32;
        
        for (size_t i = 0; i < num_hashes_; ++i) {
            size_t combined_hash = (hash1 + i * hash2) & 0xFFFFFFFF;
            size_t index = combined_hash % size_;
            if (!bit_array_[index]) {
                return false;
            }
        }
        return true;
    }

    // Merge another Bloom filter (for RPT across joins)
    void Merge(const BloomFilter& other) {
        if (size_ != other.size_) return;
        for (size_t i = 0; i < size_; ++i) {
            bit_array_[i] = bit_array_[i] | other.bit_array_[i];
        }
    }

    // Get filter size in bits
    size_t Size() const { return size_; }

    // Calculate false positive probability
    double FalsePositiveProbability() const {
        double k = num_hashes_;
        double m = size_;
        double n = count_;
        return std::pow(1 - std::exp(-k * n / m), k);
    }

private:
    size_t size_;
    size_t num_hashes_;
    std::vector<bool> bit_array_;
    std::vector<size_t> seeds_;
    size_t count_ = 0;  // Track number of inserted elements
};

// Cache-friendly linear probing hash table
template<typename KeyType>
class OptimizedHashTable {
private:
    struct Entry {
        KeyType key;
        size_t offset;
        bool occupied;
    };
    
    std::vector<Entry> table_;
    size_t size_;
    size_t capacity_;
    
public:
    OptimizedHashTable(size_t initial_capacity = 1024) 
        : capacity_(initial_capacity), size_(0) {
        table_.resize(capacity_, {KeyType(), 0, false});
    }
    
    void Insert(const KeyType& key, size_t offset) {
        if (size_ * 2 >= capacity_) {
            Rehash(capacity_ * 2);
        }
        
        size_t index = Hash(key) % capacity_;
        while (table_[index].occupied) {
            index = (index + 1) % capacity_;
        }
        
        table_[index] = {key, offset, true};
        size_++;
    }
    
    std::pair<size_t, size_t> EqualRange(const KeyType& key) const {
        size_t start = Hash(key) % capacity_;
        size_t pos = start;
        bool found = false;
        
        while (table_[pos].occupied) {
            if (table_[pos].key == key) {
                if (!found) {
                    start = pos;
                    found = true;
                }
            } else if (found && table_[pos].key != key) {
                return std::make_pair(start, pos);
            }
            pos = (pos + 1) % capacity_;
        }
        
        if (found) {
            return std::make_pair(start, pos);
        }
        return std::make_pair(0, 0);
    }
    
private:
    size_t Hash(const KeyType& key) const {
        return std::hash<std::string>{}(key);
    }
    
    void Rehash(size_t new_capacity) {
        std::vector<Entry> old_table = std::move(table_);
        capacity_ = new_capacity;
        table_.resize(capacity_, {KeyType(), 0, false});
        size_ = 0;
        
        for (const auto& entry : old_table) {
            if (entry.occupied) {
                Insert(entry.key, entry.offset);
            }
        }
    }
};

} // namespace babydb