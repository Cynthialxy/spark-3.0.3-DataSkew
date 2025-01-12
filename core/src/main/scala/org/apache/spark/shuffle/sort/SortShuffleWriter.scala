/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    shuffleExecutorComponents: ShuffleExecutorComponents)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /** Write a bunch of records to this task's output */
  //noinspection ScalaStyle
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      //如果map端要预聚合，定义预聚合的aggregator和内部排序规则
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      //如果map端不需要预聚合，聚合器为空，内部不排序
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    //将数据写入map（需要排序和预聚合）或buffer（不需要排序和预聚合）中
    //注意：数据在内存中始终不会排序，无论是否设置了排序规则，溢写到文件中时，才需要根据指定的排序规则，写入临时文件
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val mapOutputWriter = shuffleExecutorComponents.createMapOutputWriter(
      dep.shuffleId, mapId, dep.partitioner.numPartitions)
    //达到内存阈值时，排序合并
    sorter.writePartitionedMapOutput(dep.shuffleId, mapId, mapOutputWriter)
    //写出数据文件和索引文件
    val partitionLengths = mapOutputWriter.commitAllPartitions()
    //返回写入状态或结果信息
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    if (dep.mapSideCombine) {
      false
    } else {
      val bypassMergeThreshold: Int = conf.get(config.SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
