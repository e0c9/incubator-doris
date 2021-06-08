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

import static java.util.Objects.requireNonNull;

import java.util.List;

import com.google.common.collect.Lists;
import java.util.Objects;
import java.util.stream.Collectors;

public class BaseProcResult implements ProcResult {
    private final List<String> names;
    private final List<List<String>> rows;

    public static BaseProcResult empty(List<String> names) {
        return new BaseProcResult(names);
    }

    public static <T> BaseProcResult processResult(List<String> names, List<List<T>> rows) {
        List<List<String>> res = Lists.newArrayList();
        for (List<T> row : rows) {
            res.add(row.stream().map(Objects::toString).collect(Collectors.toList()));
        }
        return new BaseProcResult(names, res);
    }

    public static BaseProcResult createResult(List<String> names, List<List<String>> rows) {
        return new BaseProcResult(names, rows);
    }

    private BaseProcResult(List<String> names) {
        this(names, Lists.newArrayList());
    }

    private BaseProcResult(List<String> names, List<List<String>> rows) {
        this.names = requireNonNull(names, "column names is null");
        this.rows = requireNonNull(rows, "rows is null");
    }

    @Override
    public List<String> getColumnNames() {
        return names;
    }

    @Override
    public List<List<String>> getRows() {
        return rows;
    }
}
