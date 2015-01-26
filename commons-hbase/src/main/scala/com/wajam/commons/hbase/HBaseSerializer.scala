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

import com.wajam.commons.hbase.HBaseJsonSerializer._

class HBaseJsonSerializer(mainColumnFamily: String = DefaultColumnFamily,
                          typeHints: TypeHints = NoTypeHints,
                          extraColumnsFamilies: Map[TableName, Iterable[HBaseSchema.Family]] = Map(),
                          extraFormatSerializers: Traversable[Serializer[_]] = Nil)
    extends HBaseSerializer {

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

  implicit val formats = DefaultFormats.withHints(typeHints) + DateTimeSerializer ++ extraFormatSerializers + NoneJNothingSerializer

  private def getColumnFamilyObject(table: TableName, columnFamily: String): Option[HBaseSchema.Family] = {
    extraColumnsFamilies.get(table).flatMap(_.find(_.name == columnFamily))
  }

  private def fromJValue(v: JValue): Array[Byte] = v match {
    case JInt(i) => (List(IntByte) ++ BigIntValueConverter.toBytes(i)).toArray
    case JString(s) => (List(StringByte) ++ StringValueConverter.toBytes(s)).toArray
    case JDouble(d) => (List(DoubleByte) ++ DoubleValueConverter.toBytes(d)).toArray
    case JBool(b) => (List(BoolByte) ++ BooleanValueConverter.toBytes(b)).toArray
    case JObject(o) => (List(ObjectByte) ++ StringValueConverter.toBytes(Serialization.write(v))).toArray
    case JArray(List(JString("date"), JString(s))) => (List(DateByte) ++ StringValueConverter.toBytes(s)).toArray
    case JArray(o) => (List(ListByte) ++ StringValueConverter.toBytes(Serialization.write(v))).toArray
    case JNothing => null
    case JNull => null
    case _ => throw new Exception(s"Unsupported JSON type: $v (type ${v.getClass}")
  }

  private def fromBytes(bytes: Array[Byte]): JValue = if (bytes != null && bytes.size > 0) bytes(0) match {
    case IntByte => JInt(BigIntValueConverter.fromBytes(bytes.slice(1, bytes.size)))
    case StringByte => JString(StringValueConverter.fromBytes(bytes.slice(1, bytes.size)))
    case DoubleByte => JDouble(DoubleValueConverter.fromBytes(bytes.slice(1, bytes.size)))
    case BoolByte => JBool(BooleanValueConverter.fromBytes(bytes.slice(1, bytes.size)))
    case DateByte => JArray(List(JString("date"), JString(StringValueConverter.fromBytes(bytes.slice(1, bytes.size)))))
    case ObjectByte => parse(StringValueConverter.fromBytes(bytes.slice(1, bytes.size)))
    case ListByte => parse(StringValueConverter.fromBytes(bytes.slice(1, bytes.size)))
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
          case (JNothing, Some(_)) => // No action on extra column family
          case (_, None) => mutateField(mainColumnFamily, field)
          case _ => throw new Exception(s"Cannot serialize extra family '$name' value : $value (type ${value.getClass})")
        }
        case field => mutateField(mainColumnFamily, field)
      }
      case o => throw new Exception(s"Cannot serialize object: $o (type ${o.getClass})")
    }
  }

  def deserialize[A](table: TableName, result: WrappedResult)(implicit mf: Manifest[A]): A = {
    val fields = (result.columns(mainColumnFamily).map((colName: String) =>
      JField(colName, fromBytes(result.get(mainColumnFamily, colName)))) ++
      result.families.flatMap {
        case family if family == mainColumnFamily => None
        case family if getColumnFamilyObject(table, family).isDefined => Some(JField(family, JObject(result.columns(family).map((colName: String) =>
          JField(colName, fromBytes(result.get(family, colName)))).toList)))
      }).toList

    JObject(fields).extract[A]
  }

}

object HBaseJsonSerializer {

  val DefaultColumnFamily = "base"

  val StringByte = 's'.toByte
  val IntByte = 'i'.toByte
  val DoubleByte = 'd'.toByte
  val BoolByte = 'b'.toByte
  val ObjectByte = 'o'.toByte
  val ListByte = 'l'.toByte
  val DateByte = 't'.toByte

  def apply(typeHints: TypeHints, tables: Iterable[HBaseSchema.Table]): HBaseJsonSerializer = {
    HBaseJsonSerializer(mainColumnFamily = DefaultColumnFamily, typeHints, tables, extraFormatSerializers = Nil)
  }

  def apply(typeHints: TypeHints, tables: Iterable[HBaseSchema.Table],
            extraFormatSerializers: Traversable[Serializer[_]]): HBaseJsonSerializer = {
    HBaseJsonSerializer(mainColumnFamily = DefaultColumnFamily, typeHints, tables, extraFormatSerializers)
  }

  def apply(mainColumnFamily: String, typeHints: TypeHints, tables: Iterable[HBaseSchema.Table],
            extraFormatSerializers: Traversable[Serializer[_]]): HBaseJsonSerializer = {
    val tablesColumnsFamilies = tables.flatMap { table =>
      table.families.filter(_.name != mainColumnFamily) match {
        case Nil => None
        case families => Some(table.name -> families)
      }
    }.toMap

    new HBaseJsonSerializer(mainColumnFamily, typeHints, tablesColumnsFamilies, extraFormatSerializers)
  }

}
