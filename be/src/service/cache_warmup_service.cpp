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

#include "service/cache_warmup_service.h"
#include "common/config.h"
#include "gutil/sysinfo.h"
#include "http/http_client.h"
#include "rapidjson/document.h"
#include "util/threadpool.h"
#include <boost/algorithm/string/predicate.hpp>
#include <chrono>

const std::string kPageCacheFileNamePrefix = "cache_key_";

namespace doris {

    std::atomic_size_t LRUCacheWarmupService::success(0);
    std::atomic_size_t LRUCacheWarmupService::total(0);

    void LRUCacheWarmupService::write_shard_cache_file(int shard, VectorSerialization<StoragePageCache::CacheKey>& shard_keys) {
        namespace fs = boost::filesystem;
        fs::path dir(config::page_cache_dump_path);
        fs::path tmp_filename(kPageCacheFileNamePrefix + kCacheKeyVersion + "_shard_" + std::to_string(shard) + "_.tmp");
        fs::path tmp_file_path = dir / tmp_filename;

        fs::path current_filename(kPageCacheFileNamePrefix + kCacheKeyVersion + "_shard_" + std::to_string(shard) + "_.current");
        fs::path current_file_path = dir / current_filename;

        fs::remove(tmp_file_path);
        std::ofstream ofs(tmp_file_path.string());
        shard_keys.serialization(ofs);
        fs::rename(tmp_file_path, current_file_path);
    }

    void LRUCacheWarmupService::dump_cache_to_disk() {
        LOG(INFO) << "Start to dump cache.";
        auto start_time = std::chrono::high_resolution_clock::now();

        const auto& cache = StoragePageCache::instance();
        std::vector<VectorSerialization<StoragePageCache::CacheKey>> keys;
        cache->keys(keys);
        
        auto start_dump_time = std::chrono::high_resolution_clock::now();
        std::unique_ptr<ThreadPool> thread_pool;
        Status status = ThreadPoolBuilder("dump_cache_pool")
                .set_min_threads(keys.size())
                .set_max_threads(keys.size())
                .set_idle_timeout(MonoDelta::FromMicroseconds(2000))
                .build(&thread_pool);

        if (!status.ok()) {
            LOG(ERROR) << "Build dump cache thread pool failed, " << status.get_error_msg();
            return;
        }
        
        size_t total_keys = 0;
        for (int i = 0; i < keys.size(); i++) {
            const auto& shard_keys = keys[i];
            total_keys += shard_keys.size();
            thread_pool->submit_func(std::bind(&write_shard_cache_file, i, shard_keys));
        }
        thread_pool->wait();

        auto end_time = std::chrono::high_resolution_clock::now();
        auto time_of_writing_file = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_dump_time);
        auto time_of_getting_keys = std::chrono::duration_cast<std::chrono::milliseconds>(start_dump_time - start_time);
        
        LOG(INFO) << "Finish dumping cache, getting " << total_keys << " keys cost "
            << time_of_getting_keys.count() << " milliseconds, writing file cost " << time_of_writing_file.count() << " milliseconds.";
    }

    void LRUCacheWarmupService::load_shard_cache_file(boost::filesystem::path& file_path) {
        std::ifstream ifs(file_path.string());
        if (ifs.fail()) {
            LOG(WARNING) << "Open page cache file failed. File not exists, path = " << file_path.string();
            return;
        }

        VectorSerialization<StoragePageCache::CacheKey> cache_keys;
        cache_keys.unserialization(ifs);

        std::unique_ptr<ThreadPool> thread_pool;
        Status status = ThreadPoolBuilder("load_shard_cache_pool")
                .set_min_threads(config::max_load_cache_thread_num_per_shard)
                .set_max_threads(config::max_load_cache_thread_num_per_shard)
                .set_idle_timeout(MonoDelta::FromMicroseconds(2000))
                .build(&thread_pool);

        if (!status.ok()) {
            LOG(ERROR) << "Build load cache thread pool failed, " << status.get_error_msg();
            return;
        }
        
        for (const auto& cache_key : cache_keys) {
            thread_pool->submit_func(std::bind(&segment_v2::PageIO::warmup_page_cache, cache_key));
        }
        thread_pool->wait(); 
        boost::filesystem::remove(file_path);
        total += cache_keys.size();
    }

    void LRUCacheWarmupService::load_cache_from_disk() {
        auto start_time = std::chrono::high_resolution_clock::now();
        boost::filesystem::path dir(doris::config::page_cache_dump_path);

        std::vector<boost::filesystem::path> cache_file_paths;
        for (const auto& entry : boost::filesystem::directory_iterator(dir)) {
            auto last_write_time = boost::filesystem::last_write_time(entry.path());
            auto now = std::chrono::system_clock::to_time_t(std::chrono::high_resolution_clock::now());
            if (now - last_write_time > config::warmup_cache_expiration_time_in_mins * 60) {
                continue;
            }

            auto filename = entry.path().filename().string();
            if (boost::algorithm::starts_with(filename, kPageCacheFileNamePrefix)
                && boost::algorithm::ends_with(filename, "current")) {
                    cache_file_paths.push_back(entry.path());
            }
        }
        
        if (cache_file_paths.empty()) {
            LOG(WARNING) << "No cache files, not need to load cache.";
            return;
        }

        std::unique_ptr<ThreadPool> thread_pool;
        Status status = ThreadPoolBuilder("load_cache_pool")
                .set_min_threads(cache_file_paths.size())
                .set_max_threads(cache_file_paths.size())
                .set_idle_timeout(MonoDelta::FromMicroseconds(2000))
                .build(&thread_pool);

        if (!status.ok()) {
            LOG(ERROR) << "Build load cache thread pool failed, " << status.get_error_msg();
            return;
        }
        
        for (const auto& cache_file_path : cache_file_paths) {
            thread_pool->submit_func(std::bind(&load_shard_cache_file, cache_file_path));
        }
        thread_pool->wait();

        auto end_time = std::chrono::high_resolution_clock::now();
        auto cost_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        LOG(INFO) << "Finish cache warm-up. Success rate " << success / (double) total  << ", load " <<  total << " keys, "
            << total - success << " failed and cost " << cost_time.count() << " milliseconds.";
        
    }
}
