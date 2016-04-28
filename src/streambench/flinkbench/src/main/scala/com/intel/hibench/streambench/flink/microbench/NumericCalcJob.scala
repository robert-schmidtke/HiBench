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

class NumericCalcJob(subClassParams: ParameterTool) extends RunBenchJobWithInit(subClassParams) {
  override def processStreamData[W <: Window](lines: WindowedStream[String, Int, W], env: StreamExecutionEnvironment) {
    val cur: DataStream[(Long, Long, Long, Long)] = lines.fold[(Long, Long, Long, Long)]((Long.MinValue, Long.MaxValue, 0L, 0L), new RichFoldFunction[String, (Long, Long, Long, Long)] {
      var separator = "\\s+"
      var index = 1

      override def open(configuration: Configuration) = {
        val params = ParameterTool.fromMap(getRuntimeContext.getExecutionConfig.getGlobalJobParameters.toMap)
        separator = params.get("hibench.streamingbench.separator", separator)
        index = params.getInt("hibench.streamingbench.field_index", index)
      }

      override def fold(m: (Long, Long, Long, Long), line: String): (Long, Long, Long, Long) = {
        val splits = line.trim.split(separator)
        if (index < splits.length) {
          val num = splits(index).toLong
          (Math.max(m._1, num), Math.min(m._2, num), m._3 + num, m._4 + 1)
        } else
          m
      }
    })

    cur.addSink(new RichSinkFunction[(Long, Long, Long, Long)] {
      var reportDir = "./"
      var history_statistics: OperatorState[(Long, Long, Long, Long)] = null

      override def open(configuration: Configuration) = {
        val params = ParameterTool.fromMap(getRuntimeContext.getExecutionConfig.getGlobalJobParameters.toMap)
        reportDir = params.get("hibench.report.dir", reportDir)
        history_statistics = getRuntimeContext.getKeyValueState("history_statistics", classOf[(Long, Long, Long, Long)], (Long.MinValue, Long.MaxValue, 0L, 0L))
      }

      override def invoke(c: (Long, Long, Long, Long)) {
        val hs = history_statistics.value
        history_statistics.update(Math.max(hs._1, c._1), Math.min(hs._2, c._2), hs._3 + c._3, hs._4 + c._4)
        BenchLogUtil.logMsg("Current max: " + Math.max(hs._1, c._1), reportDir)
        BenchLogUtil.logMsg("Current min: " + Math.min(hs._2, c._2), reportDir)
        BenchLogUtil.logMsg("Current sum: " + (hs._3 + c._3), reportDir)
        BenchLogUtil.logMsg("Current total: " + (hs._4 + c._4), reportDir)
        Unit
      }
    })
  }
}
