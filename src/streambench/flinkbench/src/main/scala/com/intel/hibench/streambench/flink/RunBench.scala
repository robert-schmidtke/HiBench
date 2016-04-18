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
import com.intel.hibench.streambench.flink.util._
import com.intel.hibench.streambench.flink.microbench._

object RunBench {
  def main(args: Array[String]) {
    this.run(args)
  }

  def run(args: Array[String]) {
    if (args.length < 1) {
      BenchLogUtil.handleError("Usage: RunBench <ConfigFile>")
    }

    val params = ConfigLoader.loadParameters(args(0))
    val benchName = params.get("hibench.streamingbench.benchname")

    println(s"params:$params")
    benchName match {
      /* case "project" =>
        val fieldIndex = conf.getProperty("hibench.streamingbench.field_index").toInt
        val separator = conf.getProperty("hibench.streamingbench.separator")
        val ProjectTest = new StreamProjectionJob(param, fieldIndex, separator)
        ProjectTest.run()
      case "sample" =>
        val prob = conf.getProperty("hibench.streamingbench.prob").toDouble
        val SampleTest = new SampleStreamJob(param, prob)
        SampleTest.run() */
      case "statistics" =>
        val numericCalc = new NumericCalcJob(params)
        numericCalc.run()
      /* case "wordcount" =>
        val separator = conf.getProperty("hibench.streamingbench.separator")
        val wordCount = new Wordcount(param, separator)
        wordCount.run()
      case "grep" =>
        val pattern = conf.getProperty("hibench.streamingbench.pattern")
        val GrepStream = new GrepStreamJob(param, pattern)
        GrepStream.run()
      case "distinctcount" =>
        val fieldIndex = conf.getProperty("hibench.streamingbench.field_index").toInt
        val separator = conf.getProperty("hibench.streamingbench.separator")
        val distinct = new DistinctCountJob(param, fieldIndex, separator)
        distinct.run() */
      case _ =>
        val emptyTest = new IdentityJob(params)
        emptyTest.run()
    }
  }
}
