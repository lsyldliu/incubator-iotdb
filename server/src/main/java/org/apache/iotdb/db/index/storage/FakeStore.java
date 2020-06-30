/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.index.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iotdb.db.index.FloatDigest;

/**
 * this fake store has only one cf and one
 */
public class FakeStore {

  private Map<String, TreeMap<Long, FixWindowPackage>> data = new HashMap<>();
  private Map<String, TreeMap<Long, FloatDigest>> digests = new HashMap<>();

  public FloatDigest[] getDigests(String key, Long[] timeStamps) {
    List<FloatDigest> list = new ArrayList<>();
    Map<Long, FloatDigest> map = digests.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    FloatDigest pkg;
    for (long time : timeStamps) {
      if ((pkg = map.get(time)) != null) {
        list.add(pkg);
      }
    }
    return list.toArray(new FloatDigest[]{});
  }

  public FloatDigest getBeforeOrEqualDigest(String key, long timestamp) {
    TreeMap<Long, FloatDigest> map = digests.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    if (map.floorEntry(timestamp) == null) {
      return null;
    }
    return map.floorEntry(timestamp).getValue();
  }

  public FixWindowPackage getBeforeOrEqualPackage(String key, long timestamp) {
    TreeMap<Long, FixWindowPackage> map = data.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    if (map.floorEntry(timestamp) == null) {
      return null;
    }
    return map.floorEntry(timestamp).getValue();
  }

  public FloatDigest getAfterOrEqualDigest(String key, long timestamp) {
    TreeMap<Long, FloatDigest> map = digests.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    if (map.ceilingKey(timestamp) == null) {
      return null;
    }
    return map.ceilingEntry(timestamp).getValue();
  }

  public FloatDigest getLatestDigest(String key) throws Exception {
    TreeMap<Long, FloatDigest> map = digests.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    return map.lastEntry().getValue();
  }

  public void write(String key, long startTimestamp, FixWindowPackage dp)
      throws Exception {
    TreeMap<Long, FixWindowPackage> map = data.computeIfAbsent(key, k -> new TreeMap<>());
    map.put(startTimestamp, dp);
  }

  public void write(String key, long startTimestamp, FloatDigest digest)
      throws Exception {
    TreeMap<Long, FloatDigest> map = this.digests.computeIfAbsent(key, k -> new TreeMap<>());
    map.put(startTimestamp, digest);
  }

}
