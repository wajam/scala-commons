package com.wajam.mysql

import com.wajam.commons.Logging
import java.sql._
import collection.mutable.ArrayBuffer
import com.yammer.metrics.scala.Instrumented
import scala.util.{Failure, Success, Try}
import scala.collection.generic.CanBuildFrom
import com.wajam.tracing.Traced
import scala.annotation.tailrec

import scala.language.higherKinds

/**
 * MySQL database access helper
 */
class MysqlDatabaseAccessor(configuration: MysqlDatabaseAccessorConfig) extends Logging with Instrumented with Traced {

  private val datasources = new MysqlDatasourceAccessor(configuration)

  private lazy val metricSelect = tracedTimer(configuration.dbname + "-mysql-select")
  private lazy val metricInsert = tracedTimer(configuration.dbname + "-mysql-insert")
  private lazy val metricUpdate = tracedTimer(configuration.dbname + "-mysql-update")
  private lazy val metricDelete = tracedTimer(configuration.dbname + "-mysql-delete")

  private lazy val masterConnectionFailedAttempt = metrics.meter(configuration.dbname + "-master-connection-attempt-failed", "failed-attempt")

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
    datasources.shutdown()
  }

  private def innerSelect[T](sql: String, forWrite: Boolean, callback: (ResultSet) => Option[T], args: Any*): Option[T] = {

    def trySelect(tryNb: Int): Try[DatabaseResult] = {
      var optCon: Option[Connection] = None
      var optStatement: Option[PreparedStatement] = None
      val result: Try[DatabaseResult] = Try {
        optCon = Option(getConnection(!forWrite))
        optCon match {
          case Some(con) => {
            optStatement = Option(prepareStatement(con, sql, Statement.NO_GENERATED_KEYS, args: _*))
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
      trySelect(if (forWrite) 1 else MysqlDatabaseAccessor.SELECT_MAX_TRY)
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
        statement = prepareStatement(con, sql, Statement.RETURN_GENERATED_KEYS, args: _*)

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
        statement = prepareStatement(con, sql, Statement.NO_GENERATED_KEYS, args: _*)

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
        statement = prepareStatement(con, sql, Statement.NO_GENERATED_KEYS)

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

  /**
   * @param generateKeys Statement.RETURN_GENERATED_KEYS or Statement.NO_GENERATED_KEYS
   */
  private def prepareStatement(connection: Connection, sql: String, generateKeys: Int, args: Any*) = {

    val statement = connection.prepareStatement(sql, generateKeys)
    statement.setFetchSize(100)
    args.zipWithIndex foreach {
      case (p, i) => statement.setObject(i + 1, p)
    }

    statement
  }

  private def getConnection(readOnly: Boolean): Connection = {

    @tailrec
    def testAndGetMasterConnection(retry: Int = MysqlDatabaseAccessor.SELECT_MAX_TRY): Connection = {
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
        if (datasources.hasSlaves)
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
    Option(datasources.innerGetSlaveDataSource) match {
      case None => null
      case Some((index, ds)) => try {
        ds.getConnection
      } catch {
        case e: Exception => {
          datasources.slaveConnectionFailedAttempt(index).mark()
          throw e
        }
      }
    }
  }

  private def getMasterConnection: Connection = {
    try {
      datasources.masterDatasource.getConnection
    } catch {
      case e: Exception => {
        masterConnectionFailedAttempt.mark()
        throw e
      }
    }

  }

  class DatabaseResult(results: ResultSet, statement: PreparedStatement, connection: Connection) {

    def close() {
      MysqlDatabaseAccessor.close(results)
      MysqlDatabaseAccessor.close(statement)
      MysqlDatabaseAccessor.close(connection)
    }

    def getResults: ResultSet = {
      results
    }
  }

}

object MysqlDatabaseAccessor extends Logging {
  def close(closable: AutoCloseable) {
    try {
      if (closable != null) {
        closable.close()
      }
    } catch {
      case e: Exception => log.info("Could not close: {} with exception {}", closable, e)
    }
  }

  class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {

    def hasNext: Boolean = !rs.isLast && !rs.isAfterLast

    def next(): ResultSet = {
      rs.next()
      rs
    }

  }

  private val SELECT_MAX_TRY = 3
}
