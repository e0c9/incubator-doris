// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string>
#include <sstream>
#include <memory>
#include <string>
#include <utility>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

#include "gutil/macros.h" // for DISALLOW_COPY_AND_ASSIGN
#include "olap/lru_cache.h"
#include "gen_cpp/segment_v2.pb.h" // for cache allocation
#include "runtime/mem_tracker.h"
#include "common/stl_serialization.h"

namespace doris {

class PageCacheHandle;

// Wrapper around Cache, and used for cache page of column data
// in Segment.
// TODO(zc): We should add some metric to see cache hit/miss rate.
class StoragePageCache {
public:
    // The unique key identifying entries in the page cache.
    // Each cached page corresponds to a specific offset within
    // a file.
    //
    // TODO(zc): Now we use file name(std::string) as a part of
    // key, which is not efficient. We should make it better later
    struct CacheKey {

        CacheKey(){}

        CacheKey(std::string fname_, int64_t offset_, uint32_t size_, segment_v2::CompressionTypePB compression_type_ = segment_v2::CompressionTypePB::NO_COMPRESSION) 
            : fname(std::move(fname_)), offset(offset_), size(size_), compression_type(compression_type_) { }
        
        CacheKey(const std::string& encode_string) {
            std::istringstream is(encode_string);
            int icompression_type = 2;;
            int fname_size = 0;
            is >> offset;
            is >> size;
            is >> verify_checksum;
            is >> icompression_type;
            is >> fname_size;
            fname = encode_string.substr(encode_string.length() - fname_size);
            if (segment_v2::CompressionTypePB_IsValid(icompression_type)) {
                compression_type = static_cast<segment_v2::CompressionTypePB>(icompression_type);
            }

        }

        std::string fname;
        int64_t offset;
        uint32_t size;
        bool verify_checksum = true;
        segment_v2::CompressionTypePB compression_type;

        // Encode to a flat binary which can be used as LRUCache's key
        // format offset + " " + size + " " + compression_type + " " + fname_size + " " + fname;
        std::string encode() const {
            std::ostringstream os;
            os << offset << " ";
            os << size << " ";
            os << verify_checksum << " ";
            os << compression_type << " ";
            os << fname.length() << " ";  
            return os.str().append(fname);
        }
    private:
        friend class boost::serialization::access;

        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & fname;
            ar & offset;
            ar & size;
            ar & verify_checksum;
            ar & compression_type;
        }
    };

    // Create global instance of this class
    static void create_global_cache(size_t capacity, int32_t index_cache_percentage);

    // Return global instance.
    // Client should call create_global_cache before.
    static StoragePageCache* instance() { return _s_instance; }

    StoragePageCache(size_t capacity, int32_t index_cache_percentage);

    // Lookup the given page in the cache.
    //
    // If the page is found, the cache entry will be written into handle.
    // PageCacheHandle will release cache entry to cache when it
    // destructs.
    //
    // Cache type selection is determined by page_type argument
    //
    // Return true if entry is found, otherwise return false.
    bool lookup(const CacheKey& key, PageCacheHandle* handle, segment_v2::PageTypePB page_type);

    // Insert a page with key into this cache.
    // Given handle will be set to valid reference.
    // This function is thread-safe, and when two clients insert two same key
    // concurrently, this function can assure that only one page is cached.
    // The in_memory page will have higher priority.
    void insert(const CacheKey& key, const Slice& data, PageCacheHandle* handle,
                segment_v2::PageTypePB page_type, bool in_memory = false);

    // Page cache available check.
    // When percentage is set to 0 or 100, the index or data cache will not be allocated.
    bool is_cache_available(segment_v2::PageTypePB page_type) {
        return _get_page_cache(page_type) != nullptr;
    }

    void keys(std::vector<VectorSerialization<CacheKey>>& keys);
private:
    StoragePageCache();
    static StoragePageCache* _s_instance;

    int32_t _index_cache_percentage = 0;
    std::unique_ptr<Cache> _data_page_cache = nullptr;
    std::unique_ptr<Cache> _index_page_cache = nullptr;

    std::shared_ptr<MemTracker> _mem_tracker = nullptr;

    Cache* _get_page_cache(segment_v2::PageTypePB page_type) {
        switch (page_type)
        {
        case segment_v2::DATA_PAGE:
            return _data_page_cache.get();
        case segment_v2::INDEX_PAGE:
            return _index_page_cache.get();
        default:
            return nullptr;
        }
    }
};

// A handle for StoragePageCache entry. This class make it easy to handle
// Cache entry. Users don't need to release the obtained cache entry. This
// class will release the cache entry when it is destroyed.
class PageCacheHandle {
public:
    PageCacheHandle() {}
    PageCacheHandle(Cache* cache, Cache::Handle* handle) : _cache(cache), _handle(handle) {}
    ~PageCacheHandle() {
        if (_handle != nullptr) {
            _cache->release(_handle);
        }
    }

    PageCacheHandle(PageCacheHandle&& other) noexcept {
        // we can use std::exchange if we switch c++14 on
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    PageCacheHandle& operator=(PageCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    Cache* cache() const { return _cache; }
    Slice data() const { return _cache->value_slice(_handle); }

private:
    Cache* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(PageCacheHandle);
};

} // namespace doris
