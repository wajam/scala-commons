package com.wajam.commons.hbase

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration

class TestConfig(config: Config) {
  def getEnvironment: String = config.getString("test.environment")

  object Hadoop {
    private val entries = config.getConfig("test.hadoop").entrySet()

    def getConfiguration = {
      import scala.collection.JavaConversions._

      val hadoopConfig = new Configuration
      entries.foreach(entry => hadoopConfig.set(entry.getKey, entry.getValue.unwrapped().toString))
      hadoopConfig
    }

  }

}
