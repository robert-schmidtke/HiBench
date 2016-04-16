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

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer081
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}

import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.util.Random

class RunBenchJobWithInit(params: ParamEntity) extends SpoutTops {

  def run() {
    if (params.testWAL) {
      throw new UnsupportedOperationException("WAL testing not supported")
    }
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    if (!params.directMode) {
      throw new UnsupportedOperationException("Only direct mode supported")
    }

    val lines: DataStream[String] = createDirectStream(env)
    val parallelism = lines.getParallelism
    val random = new Random
    val keyedLines = lines.keyBy(_ => random.nextInt(parallelism))
    val windowedLines = if (params.batchInterval > 0)
      processStreamData[TimeWindow](keyedLines.timeWindow(Time.of(params.batchInterval, TimeUnit.SECONDS)), env)
    else
      processStreamData[GlobalWindow](keyedLines.countWindow(1), env)
    
    env.execute(params.appName)
  }

  def createDirectStream(env: StreamExecutionEnvironment): DataStream[String] = {
    val kafkaParams = new Properties()
    kafkaParams.setProperty("bootstrap.server", params.brokerList)
    kafkaParams.setProperty("zookeeper.connect", params.zkHost)
    kafkaParams.setProperty("group.id", params.consumerGroup)
    kafkaParams.setProperty("auto.offset.reset", "smallest")
    kafkaParams.setProperty("socket.receive.buffer.size", "1073741824")

    println(s"Create direct kafka stream, args:$kafkaParams")
    env.addSource(new FlinkKafkaConsumer081[String](params.topic, new SimpleStringSchema(), kafkaParams))
  }

}
