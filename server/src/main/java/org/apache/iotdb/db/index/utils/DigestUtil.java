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

public class DigestUtil {

  // if left point, serialNo is the result of getRootCodeBySerialNum().
  // else, serialNo is the result of getRootCodeBySerialNum() of its left bother - 1.
  public static long serialToCode(long serialNumber) {
    if (serialNumber % 2 == 1) {
      return getRootCodeBySerialNum(serialNumber);
    } else {
      return getRootCodeBySerialNum(serialNumber - 1) + 1;
    }
  }

  /**
   * get the right leaf serial number of the code number
   */
  public static long codeToSerial(long code) {
    long begin = code / 2;
    for (long i = begin; ; ++i) {
      if (getRootCodeBySerialNum(i) == code) {
        return i;
      }
    }
  }

  // get root code number by serial number
  public static long getRootCodeBySerialNum(long serialNumber) {
    long count = 0;
    long serialNum = serialNumber;
    for (; serialNum != 0; ++count) {
      serialNum &= (serialNum - 1);
    }
    return (serialNumber << 1) - count;
  }

  /**
   * depth=depth of tree - 1
   */
  public static long getDistanceBetweenLeftestAndRoot(long depth) {
    // 2^(depth+1)
    long nodesOfTree = 2 << depth;
    return nodesOfTree - 2L;
  }

  public static long getLeftestCode(long root, long rightestCode) {
    long depth = root - rightestCode;
    return root - getDistanceBetweenLeftestAndRoot(depth);
  }
}