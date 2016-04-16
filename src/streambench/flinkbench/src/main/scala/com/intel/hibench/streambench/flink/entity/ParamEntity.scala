package com.intel.hibench.streambench.flink.entity

case class ParamEntity(
  jobManager: String,
  appName: String,
  batchInterval: Int,
  zkHost: String, 
  consumerGroup: String,
  topic: String,
  threads: Int,
  recordCount: Long,
  copies: Int,
  debug: Boolean,
  totalParallel: Int
)
