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

import java.io.FileInputStream;
import java.util.Properties;

/**
 * A class that contains configuration properties for the cassandra node it runs within.
 *
 * Properties declared as volatile can be mutated via JMX.
 */
public class Config {

  public static String digest_cf = "digest";
  public static long timeWindow = 100L;

  //time series name
  public static String timeSeriesName = "key";

  //total number of points: Math.pow(2,total)
  public static String totals = "10";

  public static boolean writePackages = true;


  public static void load(String path) {
    if (path == null) {
      return;
    }

    Properties properties = new Properties();

    try {
      FileInputStream fileInputStream = new FileInputStream(path);
      properties.load(fileInputStream);
      properties.putAll(System.getenv());
    } catch (Exception e) {
      e.printStackTrace();
    }

    digest_cf = properties.getOrDefault("digest_cf", digest_cf).toString();
    timeSeriesName = properties.getOrDefault("timeSeriesName", timeSeriesName).toString();

    timeWindow = Long
        .parseLong(properties.getOrDefault("timeWindow", String.valueOf(timeWindow)).toString());

    writePackages = Boolean
        .parseBoolean(properties.getOrDefault("writePackages", writePackages).toString());
  }

}
