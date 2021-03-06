package com.wajam.asyncclient

import com.ning.http.client.{ FluentCaseInsensitiveStringsMap, Response }
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.native.Serialization
import org.scalacheck.Arbitrary
import org.scalatest.{ FunSuite, Matchers }
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class TestJsonOperations extends FunSuite
    with Matchers with GeneratorDrivenPropertyChecks with JsonOperations {

  protected def charset = "UTF-8"

  implicit protected def formats: DefaultFormats.type = DefaultFormats

  implicit val arbJValue: Arbitrary[JValue] = TestJsonOperations.arbJValue

  test("should have implicit conversion from JValue to Array[Byte]") {
    def convertTo(value: JValue)(implicit conv: RequestHandler[JValue]) = {
      conv.from(value)
    }

    forAll {
      value: JValue => convertTo(value) should be(Serialization.write(value).getBytes(charset))
    }
  }

  test("should have implicit conversions from Response to untyped Json response") {
    import org.mockito.Mockito._
    import org.scalatest.mock.MockitoSugar.mock
    def convertTo(value: Response)(implicit conv: ResponseHandler[JsonResponse]) = {
      conv.to(value)
    }

    forAll((code: Int, body: String) => {
      val response = mock[Response]
      when(response.getStatusCode).thenReturn(code)
      when(response.getResponseBody).thenReturn(body)
      when(response.getHeaders).thenReturn(new FluentCaseInsensitiveStringsMap())

      convertTo(response) should be(JsonResponse(code, body, Headers(response.getHeaders)))
    })
  }

  test("should have implicit conversion from case class to JValue") {
    val decomposer = implicitly[Decomposer[JValue]]
    forAll((str: String, i: Int, iList: List[Int]) => {
      val obj = TestJsonOperationsValues(str, i, iList)
      decomposer.decompose(obj) should be(obj.json)
    })
  }

}

object TestJsonOperations {
  // Taken from precog (blueeyes)
  // https://github.com/jdegoes/blueeyes/blob/master/json/src/test/scala/blueeyes/json/ArbitraryJValue.scala

  import org.scalacheck.Arbitrary.arbitrary
  import org.scalacheck.Gen._
  import org.scalacheck._

  def genJValue: Gen[JValue] = lzy(frequency((5, genSimple), (1, wrap(genArray)), (1, wrap(genObject))))

  def genJBool: Gen[JBool] = arbitrary[Boolean] map JBool

  def genJString: Gen[JString] = alphaStr map JString

  def genSimple: Gen[JValue] = oneOf(const(JNull), genJBool, genJString)

  def genArray: Gen[JValue] = genList map JArray

  def genObject: Gen[JObject] = genFieldList map { x => JObject(x) }

  def genList = Gen.containerOfN[List, JValue](listSize, genJValue)

  def genFieldList = Gen.containerOfN[List, JField](listSize, genField)

  def genField = for (name <- alphaStr; value <- genJValue; id <- choose(0, 1000000)) yield JField(name + id, value)

  def genJValueClass: Gen[Class[_ <: JValue]] = oneOf(
    JNull.getClass.asInstanceOf[Class[JValue]],
    classOf[JBool],
    classOf[JString],
    classOf[JArray],
    classOf[JObject])

  def listSize = choose(0, 5).sample.get

  implicit def arbJValue: Arbitrary[JValue] = Arbitrary(genJValue)
}

case class TestJsonOperationsValues(str: String, i: Int, iList: List[Int]) {
  def json = JObject(List(
    JField("str", JString(str)),
    JField("i", JInt(i)),
    JField("iList", JArray(iList map (JInt(_))))))
}

