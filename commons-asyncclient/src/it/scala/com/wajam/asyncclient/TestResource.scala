package com.wajam.asyncclient

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import com.ning.http.client.Response
import org.scalatest.{ FunSuite, Matchers }

class TestResource extends FunSuite with Matchers {
  test("should be able to create a simple resource") {
    import com.wajam.asyncclient.TestResource._
    val wXml = Await.result(ResourceExample.root.get(), 10.seconds)
    (wXml.elem \ "head" \ "title").text should include(testString)
  }
}

object TestResource extends AsyncClientTest {

  object ResourceExample extends ResourceModule[Nothing, WrappedXml, UntypedWrappedXml] {
    protected def client: AsyncClient = new AsyncClient(testConfig)

    implicit protected def requestHandler: RequestHandler[Nothing] = ??? // Not using it... yet

    implicit protected def responseHandler = new ResponseHandler[WrappedXml] {
      def to(response: Response) = {
        WrappedXml(xml.XML.withSAXParser(saxParserFactory.newSAXParser).loadString(response.getResponseBody))
      }
    }

    implicit protected def decomposer: Decomposer[Nothing] = ??? // Not using it... yet

    val root = RootResource

    object RootResource extends GettableResource[ExampleResponse] {
      protected val url = testUrl
      protected val name = "example"
    }

  }

  case class ExampleResponse(title: String, body: String)

  case class WrappedXml(elem: xml.Elem) extends ConvertableResponse[UntypedWrappedXml] {
    def as[A](implicit mf: Manifest[A]): UntypedWrappedXml[A] = UntypedWrappedXml[A](elem)
  }

  case class UntypedWrappedXml[A](elem: xml.Elem)

}

