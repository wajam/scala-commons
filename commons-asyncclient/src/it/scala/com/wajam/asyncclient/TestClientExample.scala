package com.wajam.asyncclient

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import com.ning.http.client.Response
import scala.xml
import scala.concurrent.Await
import scala.concurrent.duration._

class TestClientExample extends FunSuite with ShouldMatchers {

  test("should be able to get content from a website") {
    implicit object HtmlResponse extends ResponseHandler[xml.Elem] {
      // See https://github.com/dispatch/reboot/blob/master/core/src/main/scala/as/xml/elem.scala
      //Also, there is a bug with the doctype
      def to(response: Response) = xml.XML.withSAXParser(factory.newSAXParser).loadString(response.getResponseBody.dropWhile(_ != '\n'))

      private lazy val factory = {
        val spf = javax.xml.parsers.SAXParserFactory.newInstance()
        spf.setNamespaceAware(true)
        spf
      }
    }
    val client = new AsyncClient(HttpClientConfig())
    Await.result(client.get("http://www.example.org")(extractTitle), 10.seconds) should be("Example Domain")
  }

  private def extractTitle(x: xml.Elem) = (x \ "head" \ "title").text

}
