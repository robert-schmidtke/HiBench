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

package com.intel.flinkbench.sql

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.SequenceFileInputFormat

object ScalaFlinkJoinBench {
  def main(args: Array[String]) {
    if (args.length < 3){
      System.err.println(s"Usage: $ScalaFlinkJoinBench <rankings input> <uservisits input> <rankings-uservisits join output>")
      System.exit(1)
    }

    val rankingsInputPath = args(0)
    val uservisitsInputPath = args(1)
    val rankingsUservisitsOutputPath = args(2)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val job = new JobConf()
    // TODO compression for input

    // The value is a comma separated list
    val inputFormat = new SequenceFileInputFormat[LongWritable, Text]()

    case class Ranking(
      pageUrl: String,
      pageRank: Int,
      avgDuration: Int
    )
    val rankingsInput: DataSet[Ranking] =
      env.readHadoopFile(inputFormat, classOf[LongWritable], classOf[Text], rankingsInputPath, job).map[Ranking](new MapFunction[(LongWritable, Text), Ranking] {
      	override def map(value: (LongWritable, Text)) = {
      	  val splits = value._2.toString.split(",")
      	  new Ranking(splits(0), splits(1).toInt, splits(2).toInt)
      	}
      })

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    case class UserVisit(
      sourceIP: String,
      destUrl: String,
      visitDate: Long,
      adRevenue: Double,
      userAgent: String,
      countryCode: String,
      languageCode: String,
      searchWord: String,
      duration: Int
    )
    val uservisitsInput: DataSet[UserVisit] =
      env.readHadoopFile(inputFormat, classOf[LongWritable], classOf[Text], uservisitsInputPath, job).map[UserVisit](new MapFunction[(LongWritable, Text), UserVisit] {
      	override def map(value: (LongWritable, Text)) = {
      	  val splits = value._2.toString.split(",")
      	  new UserVisit(splits(0), splits(1), dateFormat.parse(splits(2)).getTime, splits(3).toDouble, splits(4), splits(5), splits(6), splits(7), splits(8).toInt)
        }
      })

    // The actual query
    // SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM
    //   rankings R JOIN
    //   (SELECT sourceIP, destURL, adRevenue FROM uservisits UV
    //     WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)
    //   ) NUV ON (R.pageURL = NUV.destURL)
    //   group by sourceIP order by totalRevenue DESC;
    val beginDate = dateFormat.parse("1999-01-01").getTime
    val endDate = dateFormat.parse("2000-01-01").getTime

    val r = rankingsInput.toTable
    val uv = uservisitsInput.toTable

    val nuv = uv.select('sourceIP, 'destUrl, 'adRevenue).where('visitDate >= beginDate && 'visitDate <= endDate)
    val join = r.join(nuv).where('pageUrl === 'destUrl)
    val result = join.select('sourceIP, 'pageRank.avg as 'avgPageRank, 'adRevenue.sum as 'totalRevenue).groupBy('sourceIP)

    case class Result(sourceIP: String, avgPageRank: Double, totalRevenue: Double)
    val finalResult = result.toDataSet[Result].sortPartition("totalRevenue", Order.DESCENDING).setParallelism(1)

    finalResult.map[(String, Double, Double)](new MapFunction[Result, (String, Double, Double)]{
      override def map(value: Result) = {
      	(value.sourceIP, value.avgPageRank, value.totalRevenue)
      }
    }).writeAsCsv(rankingsUservisitsOutputPath)

    env.execute("ScalaFlinkJoinBench")
  }
}
