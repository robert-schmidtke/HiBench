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

package com.intel.hibench.streambench.flink.util

import com.intel.hibench.streambench.flink.RunBench

import java.io.File
import java.io.PrintWriter

import org.slf4j.{Logger, LoggerFactory}

object BenchLogUtil extends Serializable {
  val LOG = LoggerFactory.getLogger(getClass)

  def logMsg(msg: String, reportDir: String) {
    val out = new PrintWriter(new File(reportDir + "/streamingbench/flink/streambenchlog.txt"))
    out.append(msg)
    out.close()
    LOG.info(msg)
  }
  
  def handleError(msg: String){
    LOG.error(msg)
    System.exit(1)
  }
}
