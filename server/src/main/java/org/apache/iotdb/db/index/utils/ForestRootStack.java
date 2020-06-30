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

package org.apache.iotdb.db.index.utils;

import java.util.Stack;
import org.apache.iotdb.tsfile.utils.Pair;

public class ForestRootStack<T> {

  private Stack<T> rootNodes = null;

  public ForestRootStack() {
    this.rootNodes = new Stack<T>();
  }

  public void push(T aRootNode) {
    if (rootNodes == null) {
      rootNodes = new Stack<T>();
    }

    rootNodes.push(aRootNode);
  }

  public Pair<T, T> popPair() {
    if (rootNodes == null) {
      return null;
    }

    T rightChild = rootNodes.pop();
    T leftChild = rootNodes.pop();
    return new Pair<>(leftChild, rightChild);
  }

  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    for (T t : rootNodes) {
      stringBuilder.append(t).append(",");
    }
    return stringBuilder.toString();
  }

  public int size() {
    return this.rootNodes.size();
  }

  public T get(int index) {
    return this.rootNodes.get(index);
  }
}