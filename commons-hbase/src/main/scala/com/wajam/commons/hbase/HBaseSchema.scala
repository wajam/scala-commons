package com.wajam.commons.hbase

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase._

import com.wajam.commons.hbase.HBaseSchema.{ SnappyCompression, Table }

abstract class HBaseSchema(admin: HBaseAdmin) {

  def tables: List[Table]

  def nuke(): Unit = {
    tables.foreach { table =>
      if (admin.tableExists(table.name)) {
        try {
          admin.disableTable(table.name)
        } catch {
          case e: TableNotEnabledException => // What? This is what I want!
          case e: TableNotFoundException =>
        }
        try {
          admin.deleteTable(table.name)
        } catch {
          case e: TableNotFoundException => // Thank you but why are you telling me?
        }
      }
    }
  }

  def create(): Unit = {
    // Create missing table namespaces
    val namespaces = admin.listNamespaceDescriptors().map(_.getName).toSet
    val tableNamespaces = tables.map(_.name.getNamespaceAsString).toSet
    tableNamespaces.diff(namespaces).foreach(ns => admin.createNamespace(NamespaceDescriptor.create(ns).build()))

    // Create missing tables or missing column families
    tables.foreach { table =>
      if (!admin.tableExists(table.name)) {
        // Create table
        val desc = new HTableDescriptor(table.name)
        table.families.foreach(family => desc.addFamily(family.toDescriptor))

        try {
          admin.createTable(desc)
        } catch {
          case e: TableExistsException => // Not again! Why?
        }
      } else {
        // Create missing column families
        val desc = admin.getTableDescriptor(table.name)
        val existingFamilies = desc.getColumnFamilies.map(_.getNameAsString).toSet
        val missingFamilies = table.families.filterNot(cf => existingFamilies(cf.name))
        if (missingFamilies.nonEmpty) {
          admin.disableTable(table.name)
          try {
            missingFamilies.foreach(family => admin.addColumn(table.name, family.toDescriptor))
          } finally {
            admin.enableTable(table.name)
          }
        }
      }
    }
  }

  def reset(delay: Long = 1000L): Unit = {
    nuke()
    Thread.sleep(delay)
    create()
  }

}

object HBaseSchema {

  case class Table(name: TableName, families: List[Family])

  object Table {
    def apply(namespace: String, name: String, families: List[Family]) = new Table(TableName.valueOf(namespace, name), families)
  }

  sealed abstract class FamilyCompression

  case object SnappyCompression extends FamilyCompression

  case object NoCompression extends FamilyCompression

  case class Family(name: String, compression: FamilyCompression = NoCompression, deleteIfFamilyFieldMissing: Boolean = true) {
    def toDescriptor: HColumnDescriptor = {
      val descriptor = new HColumnDescriptor(name)
      compression match {
        case SnappyCompression => descriptor.setCompressionType(Algorithm.SNAPPY)
        case _ =>
      }
      descriptor
    }

  }

}
