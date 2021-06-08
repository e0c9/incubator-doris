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

package org.apache.doris.common.proc;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.monitor.jvm.JvmInfo;
import org.apache.doris.monitor.jvm.JvmService;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.monitor.jvm.JvmStats.BufferPool;
import org.apache.doris.monitor.jvm.JvmStats.GarbageCollector;
import org.apache.doris.monitor.jvm.JvmStats.MemoryPool;
import org.apache.doris.monitor.jvm.JvmStats.Threads;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

public class JvmProcDir implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("Value")
            .build();

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        JvmService jvmService = new JvmService();

        List<List<String>> rows = Lists.newArrayList();
        // 1. jvm info
        JvmInfo jvmInfo = jvmService.info();
        rows.add(genRow("jvm start time", TimeUtils.longToTimeString(jvmInfo.getStartTime())));
        rows.add(genRow("jvm version info", Joiner.on(" ").join(jvmInfo.getVersion(),
                                                                     jvmInfo.getVmName(),
                                                                     jvmInfo.getVmVendor(),
                                                                     jvmInfo.getVmVersion())));

        rows.add(genRow("configured init heap size", DebugUtil.printByteWithUnit(jvmInfo.getConfiguredInitialHeapSize())));
        rows.add(genRow("configured max heap size", DebugUtil.printByteWithUnit(jvmInfo.getConfiguredMaxHeapSize())));
        rows.add(genRow("frontend pid", jvmInfo.getPid()));

        // 2. jvm stats
        JvmStats jvmStats = jvmService.stats();
        rows.add(genRow("classes loaded", jvmStats.getClasses().getLoadedClassCount()));
        rows.add(genRow("classes total loaded", jvmStats.getClasses().getTotalLoadedClassCount()));
        rows.add(genRow("classes unloaded", jvmStats.getClasses().getUnloadedClassCount()));

        rows.add(genRow("mem heap committed", DebugUtil.printByteWithUnit(jvmStats.getMem().getHeapCommitted().getBytes())));
        rows.add(genRow("mem heap used", DebugUtil.printByteWithUnit(jvmStats.getMem().getHeapUsed().getBytes())));
        rows.add(genRow("mem non heap committed", DebugUtil.printByteWithUnit(jvmStats.getMem().getNonHeapCommitted().getBytes())));
        rows.add(genRow("mem non heap used", DebugUtil.printByteWithUnit(jvmStats.getMem().getNonHeapUsed().getBytes())));

        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            rows.add(genRow("mem pool " + memPool.getName() + " used", DebugUtil.printByteWithUnit(memPool.getUsed().getBytes())));
            rows.add(genRow("mem pool " + memPool.getName() + " max", DebugUtil.printByteWithUnit(memPool.getMax().getBytes())));
            rows.add(genRow("mem pool " + memPool.getName() + " peak used", DebugUtil.printByteWithUnit(memPool.getPeakUsed().getBytes())));
            rows.add(genRow("mem pool " + memPool.getName() + " peak max", DebugUtil.printByteWithUnit(memPool.getPeakMax().getBytes())));
        }

        for (BufferPool bp : jvmStats.getBufferPools()) {
            rows.add(genRow("buffer pool " + bp.getName() + " count", bp.getCount()));
            rows.add(genRow("buffer pool " + bp.getName() + " used", DebugUtil.printByteWithUnit(bp.getUsed().getBytes())));
            rows.add(genRow("buffer pool " + bp.getName() + " capacity", DebugUtil.printByteWithUnit(bp.getTotalCapacity().getBytes())));
        }

        Iterator<GarbageCollector> gcIter = jvmStats.getGc().iterator();
        while (gcIter.hasNext()) {
            GarbageCollector gc = gcIter.next();
            rows.add(genRow("gc " + gc.getName() + " collection count", gc.getCollectionCount()));
            rows.add(genRow("gc " + gc.getName() + " collection time", gc.getCollectionTime().getMillis()));
        }

        Threads threads = jvmStats.getThreads();
        rows.add(genRow("threads count", threads.getCount()));
        rows.add(genRow("threads peak count", threads.getPeakCount()));
        rows.add(genRow("threads new count", threads.getThreadsNewCount()));
        rows.add(genRow("threads runnable count", threads.getThreadsRunnableCount()));
        rows.add(genRow("threads blocked count", threads.getThreadsBlockedCount()));
        rows.add(genRow("threads waiting count", threads.getThreadsWaitingCount()));
        rows.add(genRow("threads timed_waiting count", threads.getThreadsTimedWaitingCount()));
        rows.add(genRow("threads terminated count", threads.getThreadsTerminatedCount()));

        return BaseProcResult.createResult(TITLE_NAMES, rows);
    }

    private List<String> genRow(String key, Object value) {
        List<String> row = Lists.newArrayList();
        row.add(key);
        row.add(String.valueOf(value));
        return row;
    }
}
