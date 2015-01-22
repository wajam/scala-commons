package com.wajam.commons.hbase

import org.apache.hadoop.hbase.TableName
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization

import com.wajam.commons.hbase.HBaseClient._

trait HBaseSerializer {

  def serialize[A](table: TableName, a: A, mutations: WrappedRowMutations): Unit

  def deserialize[A](table: TableName, result: WrappedResult)(implicit mf: Manifest[A]): A

}

class HBaseJsonSerializer(defaultColumnFamily: String = "base",
                          extraFormatSerializers: Traversable[Serializer[_]] = Nil) extends HBaseSerializer {

  val STRING_BYTE = 's'.toByte
  val INT_BYTE = 'i'.toByte
  val DOUBLE_BYTE = 'd'.toByte
  val BOOL_BYTE = 'b'.toByte
  val OBJECT_BYTE = 'o'.toByte
  val LIST_BYTE = 'l'.toByte
  val DATE_BYTE = 't'.toByte
  val DATE_STR = DATE_BYTE.toString

  private[hbase] def typeHints: TypeHints = NoTypeHints

  private[hbase] def tablesColumnsFamilies: Map[TableName, Iterable[HBaseSchema.Family]] = Map()

  /**
   * Serializer with given <code>TypeHint</code>s. Useful to support persistence of polymorphic objects.
   * Example: if we have a Doc and Cat class that inherits Animal, we need to hint that
   * we may use them in a polymorphic way.
   * <p>
   * Usage: <code>serializer.withTypeHints(ShortTypeHints(List(classOf[Dog], classOf[Cat])))</code>
   */
  def withTypeHints(hints: TypeHints): HBaseJsonSerializer = {
    val families = tablesColumnsFamilies
    new HBaseJsonSerializer(defaultColumnFamily, extraFormatSerializers) {
      override private[hbase] val typeHints = hints
      override private[hbase] val tablesColumnsFamilies = families
    }
  }

  /**
   * Serializer with tables having "special" column families.
   */
  def withTables(tables: Iterable[HBaseSchema.Table]): HBaseJsonSerializer = {
    val hints = typeHints
    new HBaseJsonSerializer(defaultColumnFamily, extraFormatSerializers) {
      override private[hbase] val typeHints = hints
      override private[hbase] val tablesColumnsFamilies = tables.flatMap { table =>
        table.families.filter(_.name != defaultColumnFamily) match {
          case Nil => None
          case families => Some(table.name -> families)
        }
      }.toMap
    }
  }

  private object DateTimeSerializer extends Serializer[DateTime] {
    val isoDateFormat = ISODateTimeFormat.dateTime()
    private val Class = classOf[DateTime]

    override def serialize(implicit format: Formats) = {
      case d: DateTime => JArray(List(JString("date"), JString(d.toString(isoDateFormat))))
    }

    override def deserialize(implicit format: Formats) = {
      case (TypeInfo(Class, _), json) => json match {
        case JArray(List(JString("date"), JString(s))) => isoDateFormat.parseDateTime(s)
        case x => throw new MappingException("Can't convert " + x + " to DateTime")
      }
    }
  }

  private val NoneJNothingSerializer = FieldSerializer[AnyRef]({
    case (field, None) => Some((field, JNothing))
    case (field, _) if field.contains("$") => None // This ignore private fields
  }, PartialFunction.empty)

  private implicit lazy val formats = DefaultFormats.withHints(typeHints) + DateTimeSerializer ++ extraFormatSerializers + NoneJNothingSerializer

  private def getColumnFamilyObject(table: TableName, columnFamily: String): Option[HBaseSchema.Family] = {
    tablesColumnsFamilies.get(table).flatMap(_.find(_.name == columnFamily))
  }

  private def fromJValue(v: JValue): Array[Byte] = v match {
    case JInt(i) => (List(INT_BYTE) ++ BigIntValueConverter.toBytes(i)).toArray
    case JString(s) => (List(STRING_BYTE) ++ StringValueConverter.toBytes(s)).toArray
    case JDouble(d) => (List(DOUBLE_BYTE) ++ DoubleValueConverter.toBytes(d)).toArray
    case JBool(b) => (List(BOOL_BYTE) ++ BooleanValueConverter.toBytes(b)).toArray
    case JObject(o) => (List(OBJECT_BYTE) ++ StringValueConverter.toBytes(Serialization.write(v))).toArray
    case JArray(List(JString("date"), JString(s))) => (List(DATE_BYTE) ++ StringValueConverter.toBytes(s)).toArray
    case JArray(o) => (List(LIST_BYTE) ++ StringValueConverter.toBytes(Serialization.write(v))).toArray
    case JNothing => null
    case JNull => null
    case _ => throw new Exception(s"Unsupported JSON type: $v (type ${v.getClass}")
  }

  private def fromBytes(bytes: Array[Byte]): JValue = if (bytes != null && bytes.size > 0) bytes(0) match {
    case INT_BYTE => JInt(BigIntValueConverter.fromBytes(bytes.slice(1, bytes.size)))
    case STRING_BYTE => JString(StringValueConverter.fromBytes(bytes.slice(1, bytes.size)))
    case DOUBLE_BYTE => JDouble(DoubleValueConverter.fromBytes(bytes.slice(1, bytes.size)))
    case BOOL_BYTE => JBool(BooleanValueConverter.fromBytes(bytes.slice(1, bytes.size)))
    case DATE_BYTE => JArray(List(JString("date"), JString(StringValueConverter.fromBytes(bytes.slice(1, bytes.size)))))
    case OBJECT_BYTE => parse(StringValueConverter.fromBytes(bytes.slice(1, bytes.size)))
    case LIST_BYTE => parse(StringValueConverter.fromBytes(bytes.slice(1, bytes.size)))
  }
  else JNothing

  def serialize[A](table: TableName, a: A, mutations: WrappedRowMutations): Unit = {

    def mutateField(family: String, field: JField): Unit = {
      field match {
        case (k, JNothing) => mutations.deleteColumn(family, k)
        case (k, JArray(Nil)) => mutations.deleteColumn(family, k)
        case (k, v) => mutations.addBytes(family, k, fromJValue(v))
      }
    }

    Extraction.decompose(a) match {
      case o: JObject => o.obj.foreach {
        case field @ JField(name, value) => (value, getColumnFamilyObject(table, name)) match {
          case (uo: JObject, Some(_)) => uo.obj.foreach(mutateField(name, _))
          case (JNothing, Some(family)) if family.deleteIfFamilyFieldMissing => mutations.deleteFamily(name)
          case (JNothing, Some(_)) => // No action on column family
          case (_, None) => mutateField(defaultColumnFamily, field)
          case _ => throw new Exception(s"Cannot serialize extra family '$name' value : $value (type ${value.getClass})")
        }
        case field => mutateField(defaultColumnFamily, field)
      }
      case o => throw new Exception(s"Cannot serialize object: $o (type ${o.getClass})")
    }
  }

  def deserialize[A](table: TableName, result: WrappedResult)(implicit mf: Manifest[A]): A = {
    val fields = (result.columns(defaultColumnFamily).map((colName: String) =>
      JField(colName, fromBytes(result.get(defaultColumnFamily, colName)))) ++
      result.families.flatMap {
        case family if family == defaultColumnFamily => None
        case family if getColumnFamilyObject(table, family).isDefined => Some(JField(family, JObject(result.columns(family).map((colName: String) =>
          JField(colName, fromBytes(result.get(family, colName)))).toList)))
      }).toList

    JObject(fields).extract[A]
  }

}
