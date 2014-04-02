package com.wajam.mysql

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.wajam.commons.Logging
import java.sql._
import collection.mutable.ArrayBuffer
import com.yammer.metrics.scala.Instrumented
import scala.util.{Failure, Success, Try, Random}
import java.util.concurrent.{TimeUnit, ScheduledThreadPoolExecutor}
import java.util
import scala.collection.generic.CanBuildFrom
import com.wajam.tracing.Traced
import scala.annotation.tailrec

import scala.language.higherKinds

/**
 * MySQL database access helper
 */
class MySqlDatabaseAccessor(configuration: MysqlDatabaseAccessorConfig) extends Logging with Instrumented with Traced {

  private val MYSQL_URL: String = "jdbc:mysql://%s/%s?zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8"
  private val random = new Random
  private val servers = configuration.serverNames
  private val masterDatasource: ComboPooledDataSource = new ComboPooledDataSource
  private val databaseCredentials = new util.Properties()
  databaseCredentials.put("user", configuration.username)
  databaseCredentials.put("password", configuration.password)

  masterDatasource.setDriverClass("com.mysql.jdbc.Driver")
  masterDatasource.setJdbcUrl(String.format(MYSQL_URL, servers(0), configuration.database))
  configureDatasource(masterDatasource, isMaster = true)

  private val slaves = servers.drop(1).map(server => {
    val slaveDatasource = new ComboPooledDataSource
    slaveDatasource.setJdbcUrl(String.format(MYSQL_URL, server, configuration.database))
    configureDatasource(slaveDatasource, isMaster = false)
    new Slave(server, slaveDatasource, new com.mysql.jdbc.Driver())
  })

  if (slaves.length > 0 && configuration.slaveMonitoringEnabled) {
    new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(new Runnable {
      def run() {
        monitorSlave
      }
    }, configuration.slaveMonitoringIntervalSec,
      configuration.slaveMonitoringIntervalSec, TimeUnit.SECONDS)
  }

  private lazy val metricSelect = tracedTimer(configuration.dbname + "-mysql-select")
  private lazy val metricInsert = tracedTimer(configuration.dbname + "-mysql-insert")
  private lazy val metricUpdate = tracedTimer(configuration.dbname + "-mysql-update")
  private lazy val metricDelete = tracedTimer(configuration.dbname + "-mysql-delete")
  private val metricMasterPoolSize = metrics.gauge(configuration.dbname + "-master-connection-pool-size") {
    masterDatasource.getNumConnectionsDefaultUser
  }
  private val metricSlavesPoolSize =
    slaves.map(s => metrics.gauge(configuration.dbname + "-slave-connection-pool-size-" + s.name.replace(".", "-")) {
      s.datasource.getNumConnectionsDefaultUser
    })

  private lazy val masterConnectionFailedAttempt = metrics.meter(configuration.dbname + "-master-connection-attempt-failed", "failed-attempt")
  private lazy val slaveConnectionFailedAttempt = slaves.map(s => {
    metrics.meter(configuration.dbname + "-slave-connection-attempt-failed-" + s.name.replace(".", "-"), "failed-attempt")
  })

  def executeSelect[A](sql: String, callback: (ResultSet) => Option[A], args: Any*): Option[A] = {
    innerSelect[A](sql, false, callback, args: _*)
  }

  def executeSelectList[A, S[T] <: Seq[T]](sql: String, callback: (ResultSet) => Option[S[A]], args: Any*)
                                          (implicit cbf: CanBuildFrom[Nothing, A, S[A]]): S[A] = {
    executeSelect[S[A]](sql, callback, args: _*).getOrElse(Seq[A]().to[S])
  }

  def executeSelectForWrite[A](sql: String, callback: (ResultSet) => Option[A], args: Any*): Option[A] = {
    innerSelect[A](sql, true, callback, args: _*)
  }

  def shutdown() {
    masterDatasource.close()
    slaves.foreach(_.datasource.close())
  }

  private def hasSlaves(): Boolean = {
    servers.length > 1
  }

  private def innerSelect[T](sql: String, forWrite: Boolean, callback: (ResultSet) => Option[T], args: Any*): Option[T] = {

    def trySelect(tryNb: Int): Try[DatabaseResult] = {
      var optCon: Option[Connection] = None
      var optStatement: Option[PreparedStatement] = None
      val result: Try[DatabaseResult] = Try {
        optCon = Option(getConnection(!forWrite))
        optCon match {
          case Some(con) => {
            optStatement = Option(prepareStatement(con, sql, args: _*))
            optStatement match {
              case Some(statement) => {
                val res = statement.executeQuery()
                new DatabaseResult(res, statement, con)
              }
              case None => throw new SQLException("Could not create PreparedStatement")
            }
          }
          case None => throw new SQLException("Could not create Connection")
        }
      }

      result match {
        case Success(res) => Success(res)
        case Failure(e) => {
          log.warn("Could not execute statement: %s with args %s".format(sql, args), e)
          for (con <- optCon) close(con)
          for (statement <- optStatement) close(statement)
          if (tryNb > 1) {
            trySelect(tryNb - 1)
          } else {
            Failure(e)
          }
        }
      }
    }

    this.metricSelect time {
      trySelect(if (forWrite) 1 else MySqlDatabaseAccessor.SELECT_MAX_TRY)
    } match {
      case Success(res) => {
        try {
          callback(res.getResults)
        } catch {
          case e: Exception => {
            log.warn("An error occured while retrieving data from statement: %s with args %s".format(sql, args), e)
            throw e
          }
        } finally {
          res.close()
        }
      }
      case Failure(ex) => throw ex
    }
  }

  def executeInsert(sql: String, args: Any*): Option[Int] = {
    var con: Connection = null
    var statement: PreparedStatement = null

    try {
      this.metricInsert.time {
        con = getConnection(readOnly = false)
        statement = prepareStatement(con, sql, args: _*)

        if (statement.executeUpdate() == 1) {
          val resultSet = statement.getGeneratedKeys
          val generatedKey = if (resultSet.next()) {
            val result = resultSet.getInt(1)
            Some(result)
          } else {
            None
          }
          close(resultSet)
          generatedKey
        } else {
          None
        }
      }
    } catch {
      case e: Exception => {
        log.warn("Could not execute statement: %s with args %s".format(sql, args), e)
        throw new SQLException(e)
      }
    } finally {
      close(statement)
      close(con)
    }
  }

  def executeUpdate(sql: String, args: Any*) = {
    var con: Connection = null
    var statement: PreparedStatement = null

    try {
      this.metricUpdate.time {
        con = getConnection(readOnly = false)
        statement = prepareStatement(con, sql, args: _*)

        statement.executeUpdate()
      }
    } catch {
      case e: Exception => {
        log.warn("Could not execute statement: %s with args %s".format(sql, args), e)
        throw new SQLException(e)
      }
    } finally {
      close(statement)
      close(con)
    }

  }

  def executeDelete(sql: String): Int = {
    var con: Connection = null
    var statement: PreparedStatement = null

    try {
      this.metricDelete.time {
        con = getConnection(readOnly = false)
        statement = prepareStatement(con, sql)

        statement.executeUpdate()
      }
    } catch {
      case e: Exception => {
        log.warn("Could not execute statement: %s".format(sql), e)
        throw new SQLException(e)
      }
    } finally {
      close(statement)
      close(con)
    }

  }


  def insert(table: String, keys: Map[String, Any]) = {

    val query = new StringBuilder("INSERT INTO `")
    query.append(table).append("` (")
    if (!keys.isEmpty) query.append(keys.keySet.mkString("`", "`, `", "`"))
    query.append(") VALUES ")
    query.append((1 to keys.size).map(_ => "?").mkString("(", ",", ")"))

    val args = convertBooleanValues(keys)

    executeInsert(query.toString(), args: _*)
  }

  def update(table: String, id: (String, Any), keys: Map[String, Any]) {

    val withoutId = keys - id._1

    val query = new StringBuilder("UPDATE ")
    query.append(table).append(" SET ")
    withoutId.keySet.foreach(query.append(_).append("=?,"))
    query.setLength(query.length - 1)
    id._2 match {
      case s: String => query.append(" WHERE " + id._1 + "='" + s + "'")
      case _ => query.append(" WHERE " + id._1 + "=" + id._2)
    }


    val args = convertBooleanValues(keys)

    executeUpdate(query.toString(), args: _*)
  }

  def update(table: String, id: Map[String, Any], keys: Map[String, Any]) {

    val withoutId = keys -- id.keys

    val query = new StringBuilder("UPDATE ")
    query.append(table).append(" SET ")
    withoutId.keySet.foreach(query.append(_).append("=?,"))
    query.setLength(query.length - 1)
    query.append(" WHERE " + id.map {
      e => e._2 match {
        case s: String => e._1 + " = '" + s + "'"
        case _ => e._1 + " = " + e._2
      }
    }.mkString(" AND "))

    val args = convertBooleanValues(keys)

    try {
      executeUpdate(query.toString(), args: _*)
    } catch {
      case e: Exception => {
        val argsStr = args.mkString("('", "', '", "')")
        throw new SQLException("Query failed: \"%s\" with params %s".format(query, argsStr), e)
      }
    }
  }

  def delete(table: String, constraints: Map[String, Any]): Boolean = {
    val query = new StringBuilder("DELETE FROM ")
    query.append(table)

    val constraintsList = constraints.map(constraint => {
      val (column, value) = constraint
      value match {
        case s: String => column + " = '" + value + "'"
        case i: Int => column + " = " + value
        case l: Long => column + " = " + value
        case _ => throw new RuntimeException("Invalid type, can not build query for " + value.getClass)
      }
    })

    if (!constraintsList.isEmpty) query.append(" WHERE ").append(constraintsList.mkString(" AND "))
    executeDelete(query.toString()) > 0
  }

  def close(closable: AutoCloseable) {
    try {
      if (closable != null) {
        closable.close()
      }
    } catch {
      case e: Exception => log.info("Could not close: {} with exception {}", closable, e)
    }
  }

  private def innerList[A, S[T] <: Seq[T]](baseSelect: String,
                                           hasWhereClause: Boolean,
                                           customFilters: Seq[(String, String, String)],
                                           order: Option[(String, Boolean)],
                                           callback: (ResultSet) => Option[S[A]],
                                           args: Any*)
                                          (implicit cbf: CanBuildFrom[Nothing, A, S[A]]): S[A] = {
    var filtersJoin = if (hasWhereClause) " AND " else " WHERE "
    val query = new StringBuilder(baseSelect)
    var newArgs = ArrayBuffer[Any]()
    newArgs ++= args

    customFilters.foreach(p => {
      val (key, cond, value) = p
      if (cond == "in") {
        val inValues = value.split(';') filter {
          _ != ""
        }
        query.append(filtersJoin + key + " in (" + ("?," * inValues.size).substring(0, 2 * inValues.size - 1) + ")")
        filtersJoin = " AND "
        newArgs ++= inValues
      } else {
        query.append(filtersJoin + key + " " + cond + " ? ")
        newArgs += value
        filtersJoin = " AND "
      }
    })

    if (order.isDefined) {
      val direction = if (order.get._2) " ASC " else " DESC "
      query.append(" ORDER BY " + order.get._1 + direction)
    }
    executeSelectList(query.toString(), callback, newArgs: _*)
  }

  def list[A, S[T] <: Seq[T]](baseSelect: String,
                              hasWhereClause: Boolean,
                              customFilters: Seq[(String, String, String)],
                              order: Option[(String, Boolean)],
                              callback: (ResultSet) => Option[S[A]],
                              args: Any*)
                             (implicit cbf: CanBuildFrom[Nothing, A, S[A]]): S[A] = {
    innerList(baseSelect, hasWhereClause, customFilters, order, callback, args: _*)
  }

  private def convertBooleanValues(keys: Map[String, Any]): Seq[Any] = {
    val args = for (arg <- keys.values) yield {
      arg match {
        case b: Boolean => if (b) 1 else 0
        case _ => arg
      }
    }
    args.toSeq
  }

  private def prepareStatement(connection: Connection, sql: String, args: Any*) = {

    val statement = connection.prepareStatement(sql)
    statement.setFetchSize(100)
    args.zipWithIndex foreach {
      case (p, i) => statement.setObject(i + 1, p)
    }

    statement
  }

  private def getConnection(readOnly: Boolean): Connection = {

    @tailrec
    def testAndGetMasterConnection(retry: Int = MySqlDatabaseAccessor.SELECT_MAX_TRY): Connection = {
      var con: Connection = null
      var statement: PreparedStatement = null
      var rs: ResultSet = null

      val good = try {
        con = getMasterConnection
        statement = con.prepareStatement("SELECT 1")
        rs = statement.executeQuery()
        rs.next()
      } catch {
        case e: Exception => {
          log.warn("Could not execute test statement", e)
          false
        }
      } finally {
        if (rs != null) {
          rs.close()
        }
        if (statement != null) {
          statement.close()
        }
      }

      if (good) {
        con
      } else if (retry > 1) {
        testAndGetMasterConnection(retry - 1)
      } else {
        throw new SQLException("Could not get a valid connection from pooled DataSource on Master")
      }
    }

    if (readOnly) {
      val conn = getSlaveConnection
      if (conn != null) {
        conn
      } else {
        if (hasSlaves)
        // Slave are down, stop here to prevent master flooding.
          throw new SQLException("All slave are down, impossible to execute statement")
        else
        // Fall back to master since it's our single server.
          getMasterConnection
      }
    } else {
      testAndGetMasterConnection()
    }
  }

  private def getSlaveConnection: Connection = {
    if (slaves.length == 0) {
      null
    } else {
      val availableSlaves = slaves.filter(_.available)
      if (availableSlaves.length > 0) {
        val index = random.nextInt(availableSlaves.length)
        try {
          availableSlaves(index).datasource.getConnection
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

  private def getMasterConnection: Connection = {
    try {
      masterDatasource.getConnection
    } catch {
      case e: Exception => {
        masterConnectionFailedAttempt.mark()
        throw e
      }
    }

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

  class Slave(val name: String, val datasource: ComboPooledDataSource, val driver: Driver) {
    @volatile
    var available = true
  }

  class DatabaseResult(results: ResultSet, statement: PreparedStatement, connection: Connection) {

    def close() {
      MySqlDatabaseAccessor.close(results)
      MySqlDatabaseAccessor.close(statement)
      MySqlDatabaseAccessor.close(connection)
    }

    def getResults: ResultSet = {
      results
    }
  }

}

object MySqlDatabaseAccessor extends Logging {
  def close(closable: AutoCloseable) {
    try {
      if (closable != null) {
        closable.close()
      }
    } catch {
      case e: Exception => log.info("Could not close: {} with exception {}", closable, e)
    }
  }

  private val SELECT_MAX_TRY = 3
}

case class MysqlDatabaseAccessorConfig(dbname: String,
                                       username: String,
                                       password: String,
                                       serverNames: Seq[String],
                                       database: String,
                                       initMasterPoolSize: Int,
                                       maxMasterPoolSize: Int,
                                       initSlavePoolSize: Int,
                                       maxSlavePoolSize: Int,
                                       checkoutTimeoutMs: Int,
                                       maxIdleTimeSec: Int,
                                       slaveMonitoringEnabled: Boolean,
                                       slaveMonitoringIntervalSec: Long,
                                       numHelperThread: Int,
                                       maxQueryTimeInSec: Int)


