package com.wajam.gearman.utils

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, InputStreamReader, OutputStreamWriter }

object GearmanJson {

  def encodeAsJson(o: Any): Array[Byte] = {
    import org.json4s.JsonAST._
    import org.json4s.native.Serialization.write
    implicit val formats = org.json4s.DefaultFormats

    val contentEncoding = "utf-8"

    def encodeJson(j: JValue): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      write(j, new OutputStreamWriter(baos, contentEncoding)).close()
      baos.toByteArray
    }

    def toJValue(value: Any): JValue = {
      value match {
        case v: JValue => v
        case s: String => JString(s)
        case l: Long => JInt(l)
        case bi: BigInt => JInt(bi)
        case i: Int => JInt(i)
        case d: Double => JDouble(d)
        case b: Boolean => JBool(b)
        case seq: Seq[_] => JArray(seq.map(toJValue(_: Any)).toList)
        case map: Map[_, _] => JObject(map.map(e => JField(e._1.toString, toJValue(e._2))).toList)
        case _ => throw new RuntimeException("Invalid type, can not render json for " + value.getClass)
      }
    }

    o match {
      case s: String => s.getBytes(contentEncoding)
      case l: Seq[_] => encodeJson(toJValue(l))
      case m: Map[_, _] => encodeJson(toJValue(m))
      case v: JValue => encodeJson(v)
      case _ => throw new RuntimeException("Invalid type, can not render json for " + o.getClass)
    }
  }

  def decodeFromJson(o: Array[Byte]): Any = {
    import org.json4s.JsonAST._
    import org.json4s.native.Serialization.read
    implicit val formats = org.json4s.DefaultFormats

    val contentEncoding = "utf-8"

    def decodeJson(j: Array[Byte]): JValue = {
      val bais = new ByteArrayInputStream(j)
      val data = read[JValue](new InputStreamReader(bais, contentEncoding))
      bais.close()
      data
    }

    def toJValue(value: Any): JValue = {
      value match {
        case v: JValue => v
        case s: String => JString(s)
        case l: Long => JInt(l)
        case bi: BigInt => JInt(bi)
        case i: Int => JInt(i)
        case d: Double => JDouble(d)
        case b: Boolean => JBool(b)
        case seq: Seq[_] => JArray(seq.map(toJValue(_: Any)).toList)
        case map: Map[_, _] => JObject(map.map(e => JField(e._1.toString, toJValue(e._2))).toList)
        case _ => throw new RuntimeException("Invalid type, can not render json for " + value.getClass)
      }
    }

    def fromJValue(value: Any): Any = {
      value match {
        case JString(s) => s
        case JInt(bi) => bi
        case JDouble(d) => d
        case JBool(b) => b
        case JArray(seq) => seq
        case seq: List[Any] => seq.map(fromJValue(_))
        case JObject(obj: List[JField]) => obj.map {
          case JField(k, v) => (k, fromJValue(v))
        }.toMap
        case v => v
      }
    }

    fromJValue(decodeJson(o))
  }

}
