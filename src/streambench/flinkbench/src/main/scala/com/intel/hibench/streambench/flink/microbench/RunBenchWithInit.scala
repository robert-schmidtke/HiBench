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

import java.util.Properties
import java.util.concurrent.TimeUnit

class RunBenchJobWithInit(params:ParamEntity) extends SpoutTops {

  def run(){

    val env:StreamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment

    val lines = createStream(env)
      .keyBy("")
      .window(Time.of(params.batchInterval, TimeUnit.SECONDS))
      .every(Time.of(params.batchInterval, TimeUnit.SECONDS))
    processStreamData(lines, env)

    env.execute(params.appName)
  }

  def createStream(env:StreamExecutionEnvironment):DataStream[String] = {
    val kafkaParams=new Properties()
    kafkaParams.setProperty("zookeeper.connect", params.zkHost)
    kafkaParams.setProperty("group.id",  params.consumerGroup)
    kafkaParams.setProperty("rebalance.backoff.ms", "20000")
    kafkaParams.setProperty("zookeeper.session.timeout.ms", "20000")

    val kafkaInputs = (1 to params.threads).map{_ =>
      println(s"Create kafka input, args:$kafkaParams")
      env.addSource(new FlinkKafkaConsumer081[String](params.topic, new SimpleStringSchema(), kafkaParams))
    }

    return kafkaInputs(0).union(kafkaInputs.drop(1) : _*)
  }

}
