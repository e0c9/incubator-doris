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

package org.apache.doris.common.util;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.profile.ProfileTreeBuilder;
import org.apache.doris.common.profile.ProfileTreeNode;
import org.apache.doris.common.profile.ProfileTreePrinter;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/*
 * if you want to visit the attribute(such as queryID,defaultDb)
 * you can use profile.getInfoStrings("queryId")
 * All attributes can be seen from the above.
 * 
 * why the element in the finished profile array is not RuntimeProfile,
 * the purpose is let coordinator can destruct earlier(the fragment profile is in Coordinator) 
 * 
 */
public class ProfileManager {
    private static final Logger LOG = LogManager.getLogger(ProfileManager.class);
    private static volatile ProfileManager INSTANCE = null;
    public static final String QUERY_ID = "Query ID";
    public static final String START_TIME = "Start Time";
    public static final String END_TIME = "End Time";
    public static final String TOTAL_TIME = "Total";
    public static final String QUERY_TYPE = "Query Type";
    public static final String QUERY_STATE = "Query State";
    public static final String DORIS_VERSION = "Doris Version";
    public static final String USER = "User";
    public static final String DEFAULT_DB = "Default Db";
    public static final String SQL_STATEMENT = "Sql Statement";
    public static final String IS_CACHED = "Is Cached";

    public static final ArrayList<String> PROFILE_HEADERS = new ArrayList(
            Arrays.asList(QUERY_ID, USER, DEFAULT_DB, SQL_STATEMENT, QUERY_TYPE,
                    START_TIME, END_TIME, TOTAL_TIME, QUERY_STATE));

    private class ProfileElement {
        public Map<String, String> infoStrings = Maps.newHashMap();
        public String profileContent = "";
        public ProfileTreeBuilder builder = null;
        public String errMsg = "";
        public long totalTimeMs;
    }
    
    // only protect queryIdDeque; queryIdToProfileMap is concurrent, no need to protect
    private ReentrantReadWriteLock lock; 
    private ReadLock readLock;
    private WriteLock writeLock;

    // record the order of profiles by queryId
    private Deque<String> queryIdDeque;
    private Map<String, ProfileElement> queryIdToProfileMap; // from QueryId to RuntimeProfile
    
    public static ProfileManager getInstance() {
        if (INSTANCE == null) {
            synchronized (ProfileManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ProfileManager();
                }
            }
        }
        return INSTANCE;
    }
    
    private ProfileManager() {
        lock = new ReentrantReadWriteLock(true); 
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        queryIdDeque = new LinkedList<>();
        queryIdToProfileMap = new ConcurrentHashMap<>();
    }
    
    public ProfileElement createElement(RuntimeProfile profile) {
        ProfileElement element = new ProfileElement();
        RuntimeProfile summaryProfile = profile.getChildList().get(0).first;
        for (String header : PROFILE_HEADERS) {
            element.infoStrings.put(header, summaryProfile.getInfoString(header));
        }

        ProfileTreeBuilder builder = new ProfileTreeBuilder(profile);
        try {
            builder.build();
        } catch (Exception e) {
            element.errMsg = e.getMessage();
            LOG.debug("failed to build profile tree", e);
            return element;
        }

        element.builder = builder;
        element.totalTimeMs = summaryProfile.getTotalTimeMs();
        element.profileContent = profile.toString();
        return element;
    }
    
    public void pushProfile(RuntimeProfile profile) {
        if (profile == null) {
            return;
        }

        // filter profile that take more time than threshold
        ProfileElement element = createElement(profile);
        if (ConnectContext.get() != null) {
            long timeThreshold = ConnectContext.get().getSessionVariable().getReportQueryTimeThresholdMs();
            if (element.totalTimeMs < timeThreshold) {
                return;
            }
        }
        String queryId = element.infoStrings.get(ProfileManager.QUERY_ID);
        // check when push in, which can ensure every element in the list has QUERY_ID column,
        // so there is no need to check when remove element from list.
        if (Strings.isNullOrEmpty(queryId)) {
            LOG.warn("the key or value of Map is null, "
                    + "may be forget to insert 'QUERY_ID' column into infoStrings");
        }

        // a profile may be updated multiple times in queryIdToProfileMap,
        // and only needs to be inserted into the queryIdDeque for the first time.
        queryIdToProfileMap.put(queryId, element);
        writeLock.lock();
        try {
            if (!queryIdDeque.contains(queryId)) {
                if (queryIdDeque.size() >= Config.report_query_array_size) {
                    queryIdToProfileMap.remove(queryIdDeque.getFirst());
                    queryIdDeque.removeFirst();
                }
                queryIdDeque.addLast(queryId);
            }
        } finally {
            writeLock.unlock();
        }
    }
    
    public List<List<String>> getAllQueries() {
        List<List<String>> result = Lists.newArrayList();
        readLock.lock();
        try {
            Iterator reverse = queryIdDeque.descendingIterator();
            while (reverse.hasNext()) {
                String  queryId = (String) reverse.next();
                ProfileElement profileElement = queryIdToProfileMap.get(queryId);
                if (profileElement == null){
                    continue;
                }
                Map<String, String> infoStrings = profileElement.infoStrings;
                
                List<String> row = Lists.newArrayList();
                for (String str : PROFILE_HEADERS ) {
                    row.add(infoStrings.get(str));
                }
                result.add(row);
            }
        } finally {
            readLock.unlock();
        }
        return result;
    }
    
    public String getProfile(String queryID) {
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null) {
                return null;
            }
            
            return element.profileContent;
        } finally {
            readLock.unlock();
        }
    }

    public String getFragmentProfileTreeString(String queryID) {
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null || element.builder == null) {
                return null;
            }
            ProfileTreeBuilder builder = element.builder;
            return builder.getFragmentTreeRoot().debugTree(0, ProfileTreePrinter.PrintLevel.INSTANCE);
        } catch (Exception e) {
            LOG.warn("failed to get profile tree", e);
            return null;
        } finally {
            readLock.unlock();
        }
    }

    public ProfileTreeNode getFragmentProfileTree(String queryID) throws AnalysisException {
        ProfileTreeNode tree;
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null || element.builder == null) {
                throw new AnalysisException("failed to get fragment profile tree. err: "
                        + (element == null ? "not found" : element.errMsg));
            }
            return element.builder.getFragmentTreeRoot();
        } finally {
            readLock.unlock();
        }
    }

    public List<Triple<String, String, Long>> getFragmentInstanceList(String queryID, String fragmentId) throws AnalysisException {
        ProfileTreeBuilder builder;
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null || element.builder == null) {
                throw new AnalysisException("failed to get instance list. err: "
                        + (element == null ? "not found" : element.errMsg));
            }
            builder = element.builder;
        } finally {
            readLock.unlock();
        }

        return builder.getInstanceList(fragmentId);
    }

    public ProfileTreeNode getInstanceProfileTree(String queryID, String fragmentId, String instanceId) throws AnalysisException {
        ProfileTreeBuilder builder;
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null || element.builder == null) {
                throw new AnalysisException("failed to get instance profile tree. err: "
                        + (element == null ? "not found" : element.errMsg));
            }
            builder = element.builder;
        } finally {
            readLock.unlock();
        }

        return builder.getInstanceTreeRoot(fragmentId, instanceId);
    }
}
