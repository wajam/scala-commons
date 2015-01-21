package com.wajam.commons.hbase

import java.util.concurrent.{ ConcurrentHashMap, Executors }
import scala.collection.JavaConversions.asScalaSet
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

import com.github.nscala_time.time.Imports._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.util.Bytes

import com.wajam.commons.Closable
import com.wajam.commons.Closable.using
import com.wajam.tracing.Traced

class HBaseClient(conf: Configuration) extends Traced {

  import com.wajam.commons.hbase.HBaseClient._

  private val connection = HConnectionManager.createConnection(conf)

  private val tablesMetrics = new ConcurrentHashMap[TableName, TableMetrics]()

  def get(table: TableName, key: String): Option[WrappedResult] = withTable(table)(_.get(key))

  def get(table: TableName, key: String, families: List[String]): Option[WrappedResult] = {
    withTable(table)(_.get(key, Some(families)))
  }

  def put(table: TableName, key: String)(f: WrappedRowMutations => Unit): Unit = {
    withTable(table)(_.put(key)(f))
  }

  def scan(table: TableName, from: String, to: String): Iterator[WrappedResult] with Closable = {
    withTable(table)(_.scan(from, to))
  }

  def scan(table: TableName, from: String, to: String, families: List[String]): Iterator[WrappedResult] with Closable = {
    withTable(table)(_.scan(from, to, Some(families)))
  }

  def delete(table: TableName, key: String): Unit = withTable(table)(_.delete(key))

  def delete(table: TableName, from: String, to: String): Unit = withTable(table)(_.delete(from, to))

  def getObject[T](table: TableName, key: String)(implicit ser: HBaseSerializer, mf: Manifest[T]): Option[T] = {
    get(table, key).map(ser.deserialize[T](table, _))
  }

  def getObject[T](table: TableName, key: String, families: List[String])(implicit ser: HBaseSerializer, mf: Manifest[T]): Option[T] = {
    get(table, key, families).map(ser.deserialize[T](table, _))
  }

  def putObject[T](table: TableName, key: String, v: T)(implicit ser: HBaseSerializer, mf: Manifest[T]): Unit = {
    put(table, key) { m =>
      ser.serialize[T](table, v, m)
    }
  }

  def putObject[T](table: TableName, v: T)(implicit ser: HBaseSerializer, keyExtractor: HBaseKeyExtractor[T], mf: Manifest[T]): Unit = {
    put(table, keyExtractor.key(v)) { m =>
      ser.serialize[T](table, v, m)
    }
  }

  def scanObjects[T](table: TableName, from: String, to: String)(implicit ser: HBaseSerializer, mf: Manifest[T]): Iterator[T] with Closable =
    new DeserializerScanIterator(table, scan(table, from, to))

  def scanObjects[T](table: TableName, from: String, to: String, families: List[String])(implicit ser: HBaseSerializer, mf: Manifest[T]): Iterator[T] with Closable =
    new DeserializerScanIterator(table, scan(table, from, to, families))

  class DeserializerScanIterator[T](table: TableName, results: Iterator[WrappedResult] with Closable)(
      implicit ser: HBaseSerializer, mf: Manifest[T]) extends Iterator[T] with Closable {
    private val objs = results.map(r => Try(ser.deserialize[T](table, r)))

    def hasNext: Boolean = objs.hasNext

    def next(): T = objs.next() match {
      case Success(o) => o
      case Failure(e) => close(); throw e
    }

    def close(): Unit = results.close()
  }

  private def withTable[T](name: TableName)(f: WrappedTable => T): T = {
    val table = connection.getTable(name)
    try {
      f(new WrappedTable(table, getOrCreateTableMetrics(name)))
    } finally {
      table.close()
    }
  }

  private def getOrCreateTableMetrics(table: TableName): TableMetrics = {
    Option(tablesMetrics.get(table)).getOrElse {
      val tableMetrics = new TableMetrics(table, this)
      tablesMetrics.putIfAbsent(table, tableMetrics)
      tableMetrics
    }
  }
}

class HBaseAsyncClient(client: HBaseClient, executorsCount: Int) {

  import com.wajam.commons.hbase.HBaseClient._

  // Maximum query to hbase in parallel.
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(executorsCount))

  def get(table: TableName, key: String): Future[Option[WrappedResult]] = Future(client.get(table, key))

  def get(table: TableName, key: String, families: List[String]): Future[Option[WrappedResult]] = {
    Future(client.get(table, key, families))
  }

  def put(table: TableName, key: String)(f: WrappedRowMutations => Unit): Future[Unit] = Future(client.put(table, key)(f))

  def scan(table: TableName, from: String, to: String): Future[Iterator[WrappedResult] with Closable] = {
    Future(client.scan(table, from, to))
  }

  def scan(table: TableName, from: String, to: String, families: List[String]): Future[Iterator[WrappedResult] with Closable] = {
    Future(client.scan(table, from, to, families))
  }

  def delete(table: TableName, key: String): Future[Unit] = Future(client.delete(table, key))

  def delete(table: TableName, from: String, to: String): Future[Unit] = Future(client.delete(table, from, to))

  def getObject[T](table: TableName, key: String)(implicit ser: HBaseSerializer, mf: Manifest[T]): Future[Option[T]] = {
    Future(client.getObject(table, key))
  }

  def getObject[T](table: TableName, key: String, families: List[String])(implicit ser: HBaseSerializer, mf: Manifest[T]): Future[Option[T]] = {
    Future(client.getObject(table, key, families))
  }

  def putObject[T](table: TableName, key: String, v: T)(implicit ser: HBaseSerializer, mf: Manifest[T]): Future[Unit] = {
    Future(client.putObject(table, key, v))
  }

  def putObject[T](table: TableName, v: T)(implicit ser: HBaseSerializer, keyExtractor: HBaseKeyExtractor[T], mf: Manifest[T]): Future[Unit] = {
    Future(client.putObject(table, v))
  }

  def scanObjects[T](table: TableName, from: String, to: String)(implicit ser: HBaseSerializer, mf: Manifest[T]): Future[Iterator[T] with Closable] = {
    Future(client.scanObjects(table, from, to))
  }

  def scanObjects[T](table: TableName, from: String, to: String, families: List[String])(implicit ser: HBaseSerializer, mf: Manifest[T]): Future[Iterator[T] with Closable] =
    Future(client.scanObjects(table, from, to, families))

}

object HBaseClient {

  trait HBaseKeyExtractor[T] {
    def key(v: T): String
  }

  class WrappedTable(table: HTableInterface, metrics: TableMetrics) {
    def get(key: String, families: Option[List[String]] = None): Option[WrappedResult] = {
      val timer = metrics.getTimer.timerContext()
      try {
        val get = new Get(Bytes.toBytes(key))
        families.foreach(fams => fams.foreach(family => get.addFamily(Bytes.toBytes(family))))
        table.get(get).toOption
      } finally {
        timer.stop(familiesExtra(families).toMap + nameExtra)
      }
    }

    def put(key: String)(f: WrappedRowMutations => Unit): Unit = {
      val timer = metrics.putTimer.timerContext()
      try {
        val mutations = new RowMutations(Bytes.toBytes(key))
        f(mutations)
        table.mutateRow(mutations)
      } finally {
        timer.stop(Map(nameExtra))
      }
    }

    def scan(from: String, to: String, families: Option[List[String]] = None): Iterator[WrappedResult] with Closable = {
      val timer = metrics.scanTimer.timerContext()
      val scan = new Scan(Bytes.toBytes(from), Bytes.toBytes(to))
      families.foreach(fams => fams.foreach(family => scan.addFamily(Bytes.toBytes(family))))
      new WrappedResultIterator(table.getScanner(scan), (extra) => timer.stop(extra ++ familiesExtra(families) + nameExtra))
    }

    def delete(key: String): Unit = {
      val timer = metrics.deleteSingleTimer.timerContext()
      try {
        val delete = new Delete(Bytes.toBytes(key))
        table.delete(delete)
      } finally {
        timer.stop(Map(nameExtra))
      }
    }

    def delete(from: String, to: String): Unit = {
      val timer = metrics.deleteMultiTimer.timerContext()
      var count = 0
      try {
        val scan = new Scan(Bytes.toBytes(from), Bytes.toBytes(to))
        scan.setFilter(new FirstKeyOnlyFilter())
        using(new WrappedResultIterator(table.getScanner(scan), (extra: Map[String, String]) => _)) { scanner =>
          val deleteList = new java.util.ArrayList[Delete](1000) // delete() requires a mutable java List!!!
          while (scanner.hasNext) {
            val deletes = scanner.take(1000).map(result => new Delete(Bytes.toBytes(result.key)))
            deletes.foreach(deleteList.add)
            count += deleteList.size
            table.delete(deleteList)
            // HBase failed to delete items still present in deleteList. TODO: handle these failure?
            deleteList.clear()
          }
        }
      } finally {
        timer.stop(Map("count" -> count.toString, nameExtra))
      }
    }

    private def familiesExtra(families: Option[List[String]]): Option[(String, String)] = {
      families.map("families" -> _.mkString(","))
    }

    private def nameExtra: (String, String) = "table" -> table.getName.getNameAsString
  }

  implicit class WrappedResult(result: Result) {
    def toOption = if (!result.isEmpty) Some(this) else None

    lazy val families = result.getMap.keySet().map((n: Array[Byte]) => Bytes.toString(n)).toSet

    def key: String = Bytes.toString(result.getRow)

    def columns(family: String): Set[String] = result.getFamilyMap(Bytes.toBytes(family)).keySet().map((n: Array[Byte]) => Bytes.toString(n)).toSet

    def get(family: String, column: String): Array[Byte] = result.getValue(Bytes.toBytes(family), Bytes.toBytes(column))

    def getRowTimestamp: DateTime = new DateTime(result.rawCells().map(_.getTimestamp).max)

    /**
     * Returns the most recent timestamp per column present in this result
     */
    def getColumnsTimestamps: Map[(String, String), DateTime] = {
      result.rawCells().foldLeft(Map[(String, String), DateTime]()) {
        case (mapping, cell) =>
          val family = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
          val column = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          val k = (family, column)
          val ts = new DateTime(cell.getTimestamp)
          if (mapping.get(k).exists(_ > ts)) mapping else mapping + (k -> ts)
      }
    }

    def apply[T](family: String, column: String)(implicit converter: HBaseValueConverter[T]): Option[T] = {
      Option(get(family, column)).map(converter.fromBytes)
    }

    override def toString: String = result.toString
  }

  implicit class WrappedRowMutations(mutations: RowMutations) {

    private lazy val put: Put = {
      val p = new Put(mutations.getRow)
      mutations.add(p)
      p
    }

    private lazy val delete: Delete = {
      val d = new Delete(mutations.getRow)
      mutations.add(d)
      d
    }

    def add[T](family: String, column: String, value: T)(implicit converter: HBaseValueConverter[T]): Put = {
      addBytes(family, column, converter.toBytes(value))
    }

    def addBytes(family: String, column: String, bytes: Array[Byte]): Put = {
      put.add(Bytes.toBytes(family), Bytes.toBytes(column), bytes)
    }

    def deleteColumn(family: String, column: String): Delete = {
      delete.deleteColumns(Bytes.toBytes(family), Bytes.toBytes(column))
    }

    def deleteFamily(family: String): Delete = {
      delete.deleteFamily(Bytes.toBytes(family))
    }
  }

  // Intentionally not implicit (unlike the other wrappers) since the wrapper is greedy (i.e. read one result ahead)
  // and can create issues if the scanner is implicitly wrapped inside a loop.
  class WrappedResultIterator(scanner: ResultScanner, stopTimer: => (Map[String, String] => Unit)) extends Iterator[WrappedResult] with Closable {
    private var count = 0 // Important to be declared before the first call to getNextResult!!!
    private var nextResult: Option[Result] = getNextResult

    def hasNext: Boolean = {
      nextResult.isDefined
    }

    def next(): WrappedResult = {
      val result = nextResult
      nextResult = getNextResult
      result.get
    }

    def close(): Unit = {
      stopTimer(Map("count" -> count.toString))
      scanner.close()
    }

    private def getNextResult: Option[Result] = Try(Option(scanner.next)) match {
      case Success(None) =>
        close(); None
      case Success(result @ Some(_)) =>
        count += 1; result
      case Failure(e) => close(); throw e
    }
  }

  trait HBaseValueConverter[T] {
    def fromBytes(bytes: Array[Byte]): T

    def toBytes(value: T): Array[Byte]
  }

  implicit object StringValueConverter extends HBaseValueConverter[String] {
    def fromBytes(bytes: Array[Byte]): String = Bytes.toString(bytes)

    def toBytes(value: String): Array[Byte] = Bytes.toBytes(value)
  }

  implicit object BigIntValueConverter extends HBaseValueConverter[BigInt] {
    def fromBytes(bytes: Array[Byte]): BigInt = BigInt(Bytes.toString(bytes))

    def toBytes(value: BigInt): Array[Byte] = Bytes.toBytes(value.toString())
  }

  implicit object DoubleValueConverter extends HBaseValueConverter[Double] {
    def fromBytes(bytes: Array[Byte]): Double = Bytes.toString(bytes).toDouble

    def toBytes(value: Double): Array[Byte] = Bytes.toBytes(value.toString)
  }

  implicit object BooleanValueConverter extends HBaseValueConverter[Boolean] {
    def fromBytes(bytes: Array[Byte]): Boolean = Bytes.toString(bytes) == "true"

    def toBytes(value: Boolean): Array[Byte] = Bytes.toBytes(if (value) "true" else "false")
  }

  class TableMetrics(name: TableName, traced: Traced) {
    lazy val getTimer = traced.tracedTimer("get", name.getNameAsString)
    lazy val putTimer = traced.tracedTimer("put", name.getNameAsString)
    lazy val deleteSingleTimer = traced.tracedTimer("delete-single", name.getNameAsString)
    lazy val deleteMultiTimer = traced.tracedTimer("delete-multi", name.getNameAsString)
    lazy val scanTimer = traced.tracedTimer("scan", name.getNameAsString)
  }

}
