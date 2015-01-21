package com.wajam.commons.hbase

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.HBaseAdmin

object TestContext {
  lazy val testConfig = new TestConfig(ConfigFactory.load().withFallback(ConfigFactory.load("reference")))

  lazy val hadoopConfig = testConfig.Hadoop.getConfiguration

  lazy val hbaseClient = new HBaseClient(hadoopConfig)

  lazy val hbaseAdmin = new HBaseAdmin(hadoopConfig)

  lazy val hbaseSchema = new TestSchema(testConfig.getEnvironment, hbaseAdmin)

  lazy val testStore = new TestStore(hbaseClient, hbaseSchema)
}
