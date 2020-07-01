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

import java.util.TreeMap;
import org.apache.iotdb.tsfile.utils.Pair;

public class FixWindowPackage {

  protected String key = null;
  private Pair<Long, Long> timeWindow;
  private TreeMap<Long, Object> treeMap;

  public FixWindowPackage(String key, Pair<Long, Long> timeWindow) {
    this.key = key;
    this.timeWindow = timeWindow;
    treeMap = new TreeMap<>();
  }

  public void add(Pair<Long, Float> data) {
    treeMap.put(data.left, data.right);
  }

  public void add(long time, float value) {
    treeMap.put(time, value);
  }

  public boolean isEmpty() {
    return treeMap.isEmpty();
  }

  public boolean cover(Pair<Long, Float> data) {
    return data.left >= timeWindow.left && data.left < timeWindow.right;
  }

  public boolean cover(long time) {
    return time >= timeWindow.left && time < timeWindow.right;
  }

  public long getStartTime() {
    return timeWindow.left;
  }

  public long getEndTime() {
    return timeWindow.right;
  }

  public int size() {
    return treeMap.size();
  }

  public TreeMap<Long, Object> getData() {
    return treeMap;
  }

  @Override
  public String toString() {
    return treeMap.toString();
  }

}
