package com.wajam.mysql

import java.util
import java.sql._
import java.util.concurrent.{ TimeUnit, ScheduledThreadPoolExecutor }
import javax.sql.DataSource
import scala.util.{ Failure, Success, Try, Random }
import com.wajam.commons.Logging
import com.wajam.tracing.Traced
import com.mchange.v2.c3p0.ComboPooledDataSource
import com.yammer.metrics.scala.Instrumented

class MysqlDatasourceAccessor(configuration: MysqlDatabaseAccessorConfig) extends Logging with Instrumented with Traced {

  private val MYSQL_URL: String = "jdbc:mysql://%s/%s?zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8"
  private val random = new Random
  private val servers = configuration.serverNames
  private[mysql] val masterDatasource: ComboPooledDataSource = new ComboPooledDataSource
  private val databaseCredentials = new util.Properties()
  databaseCredentials.put("user", configuration.username)
  databaseCredentials.put("password", configuration.password)

  masterDatasource.setDriverClass("com.mysql.jdbc.Driver")
  masterDatasource.setJdbcUrl(String.format(MYSQL_URL, servers(0), configuration.database))
  configureDatasource(masterDatasource, isMaster = true)

  private[mysql] val slaves = servers.drop(1).map(server => {
    val slaveDatasource = new ComboPooledDataSource
    slaveDatasource.setJdbcUrl(String.format(MYSQL_URL, server, configuration.database))
    configureDatasource(slaveDatasource, isMaster = false)
    new Slave(server, slaveDatasource, new com.mysql.jdbc.Driver())
  })

  private val metricMasterPoolSize = metrics.gauge(configuration.dbname + "-master-connection-pool-size") {
    masterDatasource.getNumConnectionsDefaultUser
  }
  private val metricSlavesPoolSize =
    slaves.map(s => metrics.gauge(configuration.dbname + "-slave-connection-pool-size-" + s.name.replace(".", "-")) {
      s.datasource.getNumConnectionsDefaultUser
    })

  private[mysql] lazy val slaveConnectionFailedAttempt = slaves.map(s => {
    metrics.meter(configuration.dbname + "-slave-connection-attempt-failed-" + s.name.replace(".", "-"), "failed-attempt")
  })

  if (slaves.length > 0 && configuration.slaveMonitoringEnabled) {
    new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(new Runnable {
      def run() {
        monitorSlave
      }
    }, configuration.slaveMonitoringIntervalSec,
      configuration.slaveMonitoringIntervalSec, TimeUnit.SECONDS)
  }

  def shutdown() {
    masterDatasource.close()
    slaves.foreach(_.datasource.close())
  }

  private def configureDatasource(datasource: ComboPooledDataSource, isMaster: Boolean) {
    datasource.setUser(configuration.username)
    datasource.setPassword(configuration.password)
    if (isMaster) {
      datasource.setInitialPoolSize(configuration.initMasterPoolSize)
      datasource.setMinPoolSize(configuration.initMasterPoolSize)
      datasource.setMaxPoolSize(configuration.maxMasterPoolSize)
    } else {
      datasource.setInitialPoolSize(configuration.initSlavePoolSize)
      datasource.setMinPoolSize(configuration.initSlavePoolSize)
      datasource.setMaxPoolSize(configuration.maxSlavePoolSize)
    }
    datasource.setCheckoutTimeout(configuration.checkoutTimeoutMs)
    datasource.setMaxIdleTime(configuration.maxIdleTimeSec)
    datasource.setNumHelperThreads(configuration.numHelperThread)
    datasource.setUnreturnedConnectionTimeout(configuration.maxQueryTimeInSec)
  }

  private def monitorSlave {
    for (s <- slaves) {
      var conn: Connection = null

      val trySelect = Try {
        val url = String.format(MYSQL_URL, s.name, configuration.database)
        conn = s.driver.connect(url, databaseCredentials)
        conn.prepareStatement("SELECT 1").executeQuery()
        conn.close()
      }

      if (conn != null) Try(conn.close())

      val wasAvailable = s.available
      s.available = trySelect.isSuccess

      trySelect match {
        case Failure(e: Exception) => {
          log.warn("Exception while executing monitoring query for %s on slave %s."
            .format(configuration.dbname, s.name), e)
          log.warn("Slave {} is down.", s.name)
        }
        case Success(_) if !wasAvailable =>
          log.warn("Slave {} is up.", s.name)
        case _ =>
      }

    }
  }

  private[mysql] val hasSlaves: Boolean = servers.nonEmpty

  def getMasterDatasource: DataSource = masterDatasource

  private[mysql] def innerGetSlaveDataSource: (Int, DataSource) = {
    if (slaves.isEmpty) {
      null
    } else {
      val availableSlaves = slaves.filter(_.available)
      if (availableSlaves.length > 0) {
        val index = random.nextInt(availableSlaves.length)
        try {
          (index, availableSlaves(index).datasource)
        } catch {
          case e: Exception => {
            slaveConnectionFailedAttempt(index).mark()
            throw e
          }
        }
      } else {
        null
      }
    }
  }

  def getSlaveDatasource: DataSource = {
    Option(innerGetSlaveDataSource) match {
      case Some((_, ds)) => ds
      case None => null
    }
  }

}

private[mysql] case class Slave(name: String, datasource: ComboPooledDataSource, driver: Driver) {
  @volatile
  var available = true
}

