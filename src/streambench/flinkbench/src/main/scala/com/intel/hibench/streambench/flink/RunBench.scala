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

package com.intel.hibench.streambench.flink

import com.intel.hibench.streambench.flink.entity.ParamEntity
import com.intel.hibench.streambench.flink.microbench._

object RunBench {
  var reportDir = ""

  def main(args: Array[String]) {
    this.run(args)
  }

  def run(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: RunBench <ConfigFile>")
      System.exit(1)
    }

    val conf = new ConfigLoader(args(0))

    val benchName = conf.getProperty("hibench.streamingbench.benchname")
    val topic = conf.getProperty("hibench.streamingbench.topic_name")
    val jobManager = conf.getProperty("hibench.flink.jobmanager")
    val batchInterval = conf.getProperty("hibench.streamingbench.batch_interval").toInt
    val zkHost = conf.getProperty("hibench.streamingbench.zookeeper.host")
    val consumerGroup = conf.getProperty("hibench.streamingbench.consumer_group")
    val kafkaThreads = conf.getProperty("hibench.streamingbench.receiver_nodes").toInt
    val recordCount = conf.getProperty("hibench.streamingbench.record_count").toLong
    val copies = conf.getProperty("hibench.streamingbench.copies").toInt
    val debug = conf.getProperty("hibench.streamingbench.debug").toBoolean
    val totalParallel = conf.getProperty("hibench.yarn.executor.num").toInt * conf.getProperty("hibench.yarn.executor.cores").toInt

    this.reportDir = conf.getProperty("hibench.report.dir")

    val param = ParamEntity(jobManager, benchName, batchInterval, zkHost, consumerGroup, topic, kafkaThreads, recordCount, copies, debug, totalParallel)
    println(s"params:$param")
    benchName match {
      case "statistics" =>
        val fieldIndex = conf.getProperty("hibench.streamingbench.field_index").toInt
        val separator = conf.getProperty("hibench.streamingbench.separator")
        val numericCalc = new NumericCalcJob(param, fieldIndex, separator)
        numericCalc.run()
      case _ =>
        val emptyTest = new IdentityJob(param)
        emptyTest.run()
    }
  }
}
