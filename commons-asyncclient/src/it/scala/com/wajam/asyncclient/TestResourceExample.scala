package com.wajam.asyncclient

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FunSuite
import net.liftweb.json.Formats
import com.ning.http.client.Response
import scala.concurrent.Await
import scala.concurrent.duration._

class TestResourceExample extends FunSuite with ShouldMatchers {
  test("should be kind") {
    import TestResourceExample._
    val wXml = Await.result(ResourceExample.root.get(), 10.seconds)
    (wXml.elem \ "head" \ "title").text should be("Example Domain")

  }
}

object TestResourceExample {

  object ResourceExample extends ResourceModule[Nothing, WrappedXml, UntypedWrappedXml] {
    protected def client: AsyncClient = new AsyncClient(HttpClientConfig())

    implicit protected def requestHandler: RequestHandler[Nothing] = ??? // Not using it... yet

    implicit protected def responseHandler = new ResponseHandler[WrappedXml] {
      def to(response: Response) = {
        WrappedXml(xml.XML.withSAXParser(factory.newSAXParser).loadString(response.getResponseBody.dropWhile(_ != '\n')))
      }

      private lazy val factory = {
        val spf = javax.xml.parsers.SAXParserFactory.newInstance()
        spf.setNamespaceAware(true)
        spf
      }
    }

    implicit protected def decomposer: Decomposer[Nothing] = ??? // Not using it... yet

    implicit protected def formats: Formats = ??? // Not using this

    val root = RootResource

    object RootResource extends GettableResource[ExampleResponse] {
      protected def url: String = "http://www.example.org"
    }

  }

  case class ExampleResponse(title: String, body: String)

  case class WrappedXml(elem: xml.Elem) extends ConvertableResponse[UntypedWrappedXml] {
    def as[A](implicit mf: Manifest[A]): UntypedWrappedXml[A] = UntypedWrappedXml[A](elem)
  }

  case class UntypedWrappedXml[A](elem: xml.Elem)

}

