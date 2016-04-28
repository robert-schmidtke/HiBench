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

package com.intel.hibench.streambench.flink.microbench

import com.intel.hibench.streambench.flink.entity.ParamEntity
import com.intel.hibench.streambench.flink.util._

import org.apache.flink.api.common.functions.RichFoldFunction
import org.apache.flink.api.common.state.OperatorState
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.Window

import org.slf4j.LoggerFactory

import scala.collection.mutable.HashMap

class NumericCalcJob(subClassParams: ParameterTool) extends RunBenchJobWithInit(subClassParams) {
  override def processStreamData[W <: Window](lines: WindowedStream[String, Int, W], env: StreamExecutionEnvironment) {
    val cur: DataStream[(Long, Long, Long, Long, Long, Long)] = lines.fold[(Long, Long, Long, Long, Long, Long)]((Long.MinValue, Long.MaxValue, 0L, 0L, 0L, 0L), new RichFoldFunction[String, (Long, Long, Long, Long, Long, Long)] {
      var separator = "\\s+"
      var index = 1
      var reportDir = "./"
      var batchInterval = 0

      override def open(configuration: Configuration) = {
        val params = ParameterTool.fromMap(getRuntimeContext.getExecutionConfig.getGlobalJobParameters.toMap)
        separator = params.get("hibench.streamingbench.separator", separator)
        index = params.getInt("hibench.streamingbench.field_index", index)
        reportDir = params.get("hibench.report.dir", reportDir)
        batchInterval = params.getInt("hibench.streamingbench.batch_interval", batchInterval)
      }

      override def fold(m: (Long, Long, Long, Long, Long, Long), line: String): (Long, Long, Long, Long, Long, Long) = {
        val currentTime = System.currentTimeMillis
        if (line.contains("+")) {
          // hostname+timestamp
          val splits = line.split("\\+")
          (m._1, m._2, m._3, m._4, m._5 + (currentTime - splits(1).toLong), m._6 + 1)
        } else {
          val splits = line.trim.split(separator)
          if (index < splits.length) {
            val num = splits(index).toLong
            (Math.max(m._1, currentTime), Math.min(m._2, currentTime - batchInterval * 1000), m._3 + num, m._4 + 1, m._5, m._6)
          } else
            m
        }
      }
    })

    cur.addSink(new RichSinkFunction[(Long, Long, Long, Long, Long, Long)] {
      var reportDir = "./"
      var recordCount = 0L
      var history_statistics = (Long.MinValue, Long.MaxValue, 0L, 0L, 0L, 0L)

      override def open(configuration: Configuration) = {
        val params = ParameterTool.fromMap(getRuntimeContext.getExecutionConfig.getGlobalJobParameters.toMap)
        reportDir = params.get("hibench.report.dir", reportDir)
        recordCount = params.getLong("hibench.streamingbench.record_count", recordCount)
      }

      override def invoke(c: (Long, Long, Long, Long, Long, Long)) {
        // only count if we're receiving relevant records
        if (c._4 > 0 && history_statistics._4 <= recordCount) {
          history_statistics = (
            Math.max(history_statistics._1, c._1), Math.min(history_statistics._2, c._2), history_statistics._3 + c._3, history_statistics._4 + c._4,
            history_statistics._5 + c._5, history_statistics._6 + c._6)
        }
        Unit
      }

      override def close() = {
        BenchLogUtil.logMsg("Latest time: " + history_statistics._1, reportDir)
        BenchLogUtil.logMsg("Earliest time: " + history_statistics._2, reportDir)
        BenchLogUtil.logMsg("Value sum: " + history_statistics._3, reportDir)
        BenchLogUtil.logMsg("Value count: " + history_statistics._4, reportDir)
        BenchLogUtil.logMsg("Duration: " + (history_statistics._1 - history_statistics._2) + "ms", reportDir)
        BenchLogUtil.logMsg("Total latency of " + history_statistics._5 + "ms over " + history_statistics._6 + " counts", reportDir)
      }
    }).setParallelism(1)
  }
}
