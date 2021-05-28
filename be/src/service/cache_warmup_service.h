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

#include<atomic>
#include <boost/filesystem.hpp>
#include "common/stl_serialization.h"
#include "olap/rowset/segment_v2/page_io.h"

namespace doris {

class LRUCacheWarmupService {
public:
    static std::atomic_size_t success;
    static std::atomic_size_t total;

    static void dump_cache_to_disk();
    static void load_cache_from_disk();

private:
    static void write_shard_cache_file(int shard, VectorSerialization<StoragePageCache::CacheKey>& shard_keys);
    static void load_shard_cache_file(boost::filesystem::path& cache_file_path);
};

}
