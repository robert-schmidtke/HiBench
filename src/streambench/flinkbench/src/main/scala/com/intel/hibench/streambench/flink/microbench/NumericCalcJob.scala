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
    val cur: DataStream[(Long, Long, Long, Long)] = lines.fold[(Long, Long, Long, Long)]((Long.MinValue, Long.MaxValue, 0L, 0L), new RichFoldFunction[String, (Long, Long, Long, Long)] {
      var separator = "\\s+"
      var index = 1
      val latencies = new HashMap[String, (Long, Long)]
      var reportDir = "./"

      override def open(configuration: Configuration) = {
        val params = ParameterTool.fromMap(getRuntimeContext.getExecutionConfig.getGlobalJobParameters.toMap)
        separator = params.get("hibench.streamingbench.separator", separator)
        index = params.getInt("hibench.streamingbench.field_index", index)
        reportDir = params.get("hibench.report.dir", reportDir)
      }

      override def fold(m: (Long, Long, Long, Long), line: String): (Long, Long, Long, Long) = {
        if (line.contains("+")) {
          // hostname+timestamp
          val splits = line.split("\\+")
          // (total time difference, count)
          val latency = latencies.get(splits(0)).getOrElse((0L, 0L))
          latencies.put(splits(0), (latency._1 + (System.currentTimeMillis - splits(1).toLong), latency._2 + 1))
          m
        } else {
          val splits = line.trim.split(separator)
          if (index < splits.length) {
            val num = splits(index).toLong
            (Math.max(m._1, num), Math.min(m._2, num), m._3 + num, m._4 + 1)
          } else
            m
        }
      }

      override def close() = {
        latencies foreach {
          case (k, (v1, v2)) => BenchLogUtil.logMsg(k + " has " + v1 + "ms latency over " + v2 + " counts", reportDir)
        }
      }
    })

    cur.addSink(new RichSinkFunction[(Long, Long, Long, Long)] {
      var reportDir = "./"
      var history_statistics = (Long.MinValue, Long.MaxValue, 0L, 0L)
      var startTime = Long.MaxValue
      var endTime = Long.MinValue

      override def open(configuration: Configuration) = {
        val params = ParameterTool.fromMap(getRuntimeContext.getExecutionConfig.getGlobalJobParameters.toMap)
        reportDir = params.get("hibench.report.dir", reportDir)
      }

      override def invoke(c: (Long, Long, Long, Long)) {
        startTime = Math.min(startTime, System.currentTimeMillis)
        history_statistics = (Math.max(history_statistics._1, c._1), Math.min(history_statistics._2, c._2), history_statistics._3 + c._3, history_statistics._4 + c._4)
        endTime = Math.max(endTime, System.currentTimeMillis)
        Unit
      }

      override def close() = {
        BenchLogUtil.logMsg("Current max: " + history_statistics._1, reportDir)
        BenchLogUtil.logMsg("Current min: " + history_statistics._2, reportDir)
        BenchLogUtil.logMsg("Current sum: " + history_statistics._3, reportDir)
        BenchLogUtil.logMsg("Current total: " + history_statistics._4, reportDir)
        BenchLogUtil.logMsg("Duration: " + (endTime - startTime) + "ms", reportDir)
      }
    }).setParallelism(1)
  }
}
