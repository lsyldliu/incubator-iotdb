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
package org.apache.iotdb.db.index;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.index.IndexException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.storage.Config;
import org.apache.iotdb.db.index.storage.FakeStore;
import org.apache.iotdb.db.index.utils.DigestUtil;
import org.apache.iotdb.db.index.utils.ForestRootStack;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PisaIndex<T extends FloatDigest> {

  private Logger logger = LoggerFactory.getLogger(PisaIndex.class);
  private long maxSerialNo = 0;

  private ForestRootStack<T> rootNodes;
  protected FakeStore reader = new FakeStore();
  protected FakeStore writer = new FakeStore();
  private String rowkey;

  private Pair<Long, Long> currentWindow = new Pair<>(0L, Config.timeWindow);

  private static final String INDEX_FILE_PATH =
      IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0] + File.separator
          + "index" + File.separator + "test.tsfile";

  public static PisaIndex getInstance() {
    return PisaIndexHolder.INSTANCE;
  }

  public boolean build(Path path) throws IndexException {
    // TODO initial
    this.rowkey = path.toString();

    T lastDigest = getLastDigest();

    if (null == lastDigest) {
      maxSerialNo = 0;
      rootNodes = new ForestRootStack<T>();
    } else {
      maxSerialNo = lastDigest.getSerial();
      rootNodes = getRoots(lastDigest);
    }

    // TODO new codes
    long queryId = QueryResourceManager.getInstance().assignQueryId(true);
    QueryContext context = new QueryContext(queryId);
    Map<Long, Statistics> timePartitionMap = new HashMap<>();

    try {
      TSDataType dataType = SchemaUtils.getSeriesTypeByPath(path);
      QueryDataSource queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSource(path, context, null);
      Set<String> allSensors = new HashSet<>();
      allSensors.add(path.getMeasurement());
      SeriesAggregateReader seriesReader = new SeriesAggregateReader(path, allSensors, dataType,
          context,
          queryDataSource, null, null, null);

      while (seriesReader.hasNextChunk()) {
        if (seriesReader.canUseCurrentChunkStatistics()) {
          Statistics chunkStatistics = seriesReader.currentChunkStatistics();
          updateStatistics(timePartitionMap, chunkStatistics);
          seriesReader.skipCurrentChunk();
          continue;
        }
        while (seriesReader.hasNextPage()) {
          if (seriesReader.canUseCurrentPageStatistics()) {
            Statistics pageStatistic = seriesReader.currentPageStatistics();
            updateStatistics(timePartitionMap, pageStatistic);
            seriesReader.skipCurrentPage();
            continue;
          }

          while (seriesReader.hasNextPage()) {
            BatchData nextOverlappedPageData = seriesReader.nextPage();
            while (nextOverlappedPageData.hasCurrent()) {
              updateStatisticsFromPage(timePartitionMap, nextOverlappedPageData, dataType);
              nextOverlappedPageData.next();
            }
            nextOverlappedPageData.resetBatchData();
          }
        }
      }
      Set<PisaIndexNode> indexNodeSet = new TreeSet<>(
          Comparator.comparingLong(PisaIndexNode::getNodeNumber));
      for (Map.Entry<Long, Statistics> entry : timePartitionMap.entrySet()) {
        PisaIndexNode node = new PisaIndexNode(entry.getKey(), entry.getValue());
        indexNodeSet.add(node);
        PisaIndexNode parentNode = mergeAndGenerateParent(indexNodeSet, node, dataType);
        while (parentNode != null) {
          indexNodeSet.add(parentNode);
          parentNode = mergeAndGenerateParent(indexNodeSet, parentNode, dataType);
        }
      }

      // TODO store PISA
      File f = FSFactoryProducer.getFSFactory().getFile(INDEX_FILE_PATH);
      if (f.exists()) {
        Files.delete(f.toPath());
      }
      try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
        tsFileWriter.registerTimeseries(new Path(path.getFullPath(), "min"),
            new MeasurementSchema("max", dataType, TSEncoding.RLE));
        tsFileWriter.registerTimeseries(new Path(path.getFullPath(), "max"),
            new MeasurementSchema("max", dataType, TSEncoding.RLE));
        tsFileWriter.registerTimeseries(new Path(path.getFullPath(), "first"),
            new MeasurementSchema("first", dataType, TSEncoding.RLE));
        tsFileWriter.registerTimeseries(new Path(path.getFullPath(), "last"),
            new MeasurementSchema("last", dataType, TSEncoding.RLE));
        tsFileWriter.registerTimeseries(new Path(path.getFullPath(), "sum"),
            new MeasurementSchema("sum", TSDataType.DOUBLE, TSEncoding.RLE));

        for (int i = 0; i < 10000; i++) {
          for (PisaIndexNode node : indexNodeSet) {
            TSRecord tsRecord = new TSRecord(node.getNodeNumber(), path.getFullPath() + i);
            tsRecord.addTuple(DataPoint
                .getDataPoint(dataType, "min", String.valueOf(node.getStatistics().getMinValue())));
            tsRecord.addTuple(DataPoint
                .getDataPoint(dataType, "max", String.valueOf(node.getStatistics().getMaxValue())));
            tsRecord.addTuple(DataPoint
                .getDataPoint(dataType, "first",
                    String.valueOf(node.getStatistics().getFirstValue())));
            tsRecord.addTuple(DataPoint
                .getDataPoint(dataType, "last",
                    String.valueOf(node.getStatistics().getLastValue())));
            tsRecord.addTuple(DataPoint
                .getDataPoint(TSDataType.DOUBLE, "sum",
                    String.valueOf(node.getStatistics().getSumValue())));

            // write TSRecord
            tsFileWriter.write(tsRecord);
          }
        }
      }

      // TODO for experiment
      // create reader and get the readTsFile interface
      Path sumPath = new Path(path + "0.sum");
      try (ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(
          new TsFileSequenceReader(INDEX_FILE_PATH))) {
        long startTime = System.nanoTime();
        Set<Long> queryNodes = getQueryNodes(1, 26);
        GlobalTimeExpression expression = new GlobalTimeExpression(
            TimeFilter.in(queryNodes, false));

        List<Path> paths = new ArrayList<>();
        paths.add(sumPath);
        QueryExpression queryExpression = QueryExpression.create(paths, expression);
        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        double sum = 0.0;
        while (queryDataSet.hasNext()) {
          sum += queryDataSet.next().getFields().get(0).getDoubleV();
        }

        logger.info("queryNodes: {}, sum: {}, cost time: {}", queryNodes, sum,
            (System.nanoTime() - startTime) / 100000);
      }
    } catch (MetadataException | StorageEngineException | IOException | WriteProcessException | QueryProcessException e) {
      throw new IndexException(e);
    }
    return true;
  }

  private void updateStatistics(Map<Long, Statistics> timePartitionMap, Statistics statistics) {
    long timePartitionId = fromTimeToTimePartition(statistics.getStartTime()) + 1;
    if (timePartitionMap.containsKey(timePartitionId)) {
      timePartitionMap.get(timePartitionId).mergeStatistics(statistics);
    } else {
      timePartitionMap.put(timePartitionId, statistics);
    }
  }

  private void updateStatisticsFromPage(Map<Long, Statistics> timePartitionMap,
      BatchData dataInThisPage, TSDataType dataType) {
    long timePartitionId = fromTimeToTimePartition(dataInThisPage.currentTime());
    Statistics statistics;
    if (timePartitionMap.containsKey(timePartitionId)) {
      statistics = timePartitionMap.get(timePartitionId);
    } else {
      statistics = Statistics.getStatsByType(dataType);
    }
    statistics.update(dataInThisPage.currentTime(), dataInThisPage.currentValue(), dataType);
    timePartitionMap.put(timePartitionId, statistics);
  }

  private long fromTimeToTimePartition(long time) {
    return time / IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
  }

  private PisaIndexNode mergeAndGenerateParent(Set<PisaIndexNode> indexNodeSet,
      PisaIndexNode node, TSDataType dataType) {
    long nodeNumber = node.getNodeNumber();
    int level = node.getLevel();
    int bias = (int) Math.pow(2, level) - 1;
    Statistics statistics = Statistics.getStatsByType(dataType);
    long parentNodeNumber = 0;
    for (PisaIndexNode indexNode : indexNodeSet) {
      if (!indexNode.isInMem() || indexNode.getLevel() != level) {
        continue;
      }
      if (indexNode.getNodeNumber() == nodeNumber - bias) {
        statistics.mergeStatistics(indexNode.getStatistics());
        parentNodeNumber = nodeNumber + 1;
        indexNode.setInMem(false);
        break;
      } else if (indexNode.getNodeNumber() == nodeNumber + bias) {
        statistics.mergeStatistics(indexNode.getStatistics());
        parentNodeNumber = nodeNumber + bias + 1;
        indexNode.setInMem(false);
        break;
      }
    }
    if (statistics.isEmpty()) {
      return null;
    }
    node.setInMem(false);
    statistics.mergeStatistics(node.getStatistics());
    return new PisaIndexNode(parentNodeNumber, level + 1, statistics);
  }

  public boolean drop(Path path) {
    return true;
  }

  private T getLastDigest() {
    T lastDigest = null;
    try {
      System.out.println("rowkey:" + rowkey);
      lastDigest = (T) reader.getLatestDigest(rowkey);
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(String.format("Read lastest digest error : (%s)", rowkey));
    }
    return lastDigest;
  }

  protected ForestRootStack<T> getRoots(T digest) {
    ForestRootStack<T> rootStack = new ForestRootStack<T>();

    List<Long> rootCodes = getRootCodes(maxSerialNo);
    Long[] codeArray = rootCodes.toArray(new Long[]{});
    if (codeArray.length != 0) {
      T[] rootArray = (T[]) reader.getDigests(rowkey, codeArray);

      for (int i = rootArray.length - 1; i >= 0; --i) {
        rootStack.push(rootArray[i]);
      }
    }
    if (digest.getSerial() % 2 == 1) {
      rootStack.push(digest);
    }

    return rootStack;
  }

  //get all root codes in memory by a serial number
  protected List<Long> getRootCodes(long serialNo) {
    List<Long> rootCodes = new ArrayList<Long>();
    long tmpUpperSerial = serialNo;

    if (tmpUpperSerial % 2 == 1) {
      tmpUpperSerial--;
    }

    while (tmpUpperSerial > 1) {
      long rightestCode = DigestUtil.serialToCode(tmpUpperSerial);
      long parentCode = DigestUtil.getRootCodeBySerialNum(tmpUpperSerial);

      //why negative??????
      rootCodes.add(-parentCode);

      long leftestCode = DigestUtil.getLeftestCode(parentCode, rightestCode);
      tmpUpperSerial = DigestUtil.codeToSerial(leftestCode) - 1;
    }

    return rootCodes;
  }

  public int insert(T digest) throws Exception {
    ++maxSerialNo;
    digest.setSerial(maxSerialNo);

    long code = DigestUtil.serialToCode(maxSerialNo);
    digest.setCode(code);

    //digest nodes to be persistent
    List<T> digests = new ArrayList<>();
    rootNodes.push(digest);
    //first add the leaf node of digest forest
    digests.add(digest);

    logger.debug("Insert Node code number : " + code);
    if (maxSerialNo % 2 == 0) {
      long parentCode = DigestUtil.getRootCodeBySerialNum(maxSerialNo);
      //new generated internal nodes
      List<T> parentNodes = generateParents(code + 1, parentCode);
      digests.addAll(parentNodes);
    }
    for (T digestNode : digests) {
      //flush all nodes in digests into disk
      writer.write(rowkey, digestNode.getStartTime(), digestNode);
    }
    return digests.size();
  }

  private List<T> generateParents(long lowCodeNum, long upCodeNum) {
    List<T> digests = new ArrayList<>();

    for (long count = lowCodeNum; count <= upCodeNum; ++count) {
      Pair<T, T> children = rootNodes.popPair();
      T parentNode = generateParent(children.left, children.right);
      logger.debug("Generate parent code number : " + parentNode.getCode());
      rootNodes.push(parentNode);
      digests.add(parentNode);
    }

    return digests;
  }

  @SuppressWarnings("unchecked")
  private T generateParent(T a, T b) {
    return (T) a.generateParent(a, b);
  }

  public void clean() {
  }

  private Set<Long> getQueryNodes(long leftLeafNumber, long rightLeafNumber) {
    long leftestLeafNumber = PisaIndexNode.getLeftestLeafNumber(rightLeafNumber);
    long rootNodeNumber = PisaIndexNode.getRootNodeNumber(rightLeafNumber);

    Set<Long> parentNodes = new HashSet<>();
    while (leftestLeafNumber >= leftLeafNumber) {
      parentNodes.add(rootNodeNumber);
      if (leftestLeafNumber == leftLeafNumber) {
        break;
      }
      rightLeafNumber = leftestLeafNumber - 1;
      rootNodeNumber = DigestUtil.getRootCodeBySerialNum(rightLeafNumber);
      leftestLeafNumber = PisaIndexNode.getLeftestLeafNumber(rightLeafNumber);
    }

    return parentNodes;
  }

  private static class PisaIndexHolder {

    static final PisaIndex INSTANCE = new PisaIndex();
  }
}