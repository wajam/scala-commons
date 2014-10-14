package com.wajam.asyncclient

import scala.concurrent.{ ExecutionContext, Future }
import com.ning.http.client.Response
import dispatch._
import com.ning.http.client
import scala.language.implicitConversions
import java.util.concurrent.ExecutionException

trait BaseAsyncClient {

  def get[Response](request: Request)(implicit handler: ResponseHandler[Response],
                                      ec: ExecutionContext): Future[Response]

  def post[RequestBody, Response](request: Request, value: RequestBody)(implicit requestHandler: RequestHandler[RequestBody],
                                                                        responseHandler: ResponseHandler[Response],
                                                                        ec: ExecutionContext): Future[Response]

  def put[RequestBody, Response](request: Request, value: RequestBody)(implicit requestHandler: RequestHandler[RequestBody],
                                                                       responseHandler: ResponseHandler[Response],
                                                                       ec: ExecutionContext): Future[Response]

  def delete[Response](request: Request)(implicit handler: ResponseHandler[Response],
                                         ec: ExecutionContext): Future[Response]
}

// --To build using an host
//  host("en.wikipedia.org") / "wiki" / "Main_Page"
//
// Will yield:
// http://en.wikipedia.org/wiki/Main_Page

// --To build using an host with https
//  secureHost("en.wikipedia.org") / "wiki" / "Main_Page"
//
// Will yield:
// https://en.wikipedia.org/wiki/Main_Page

// --To add parameters
//  secureHost("en.wikipedia.org") / "w" / "index.php" params ("search" -> "Wikipedia")
//
// Will yield:
// https://en.wikipedia.org/w/index.php?search=Wikipedia

// --To build from an url
//  url("https://en.wikipedia.org/w/index.php")
//
// Will yield:
// https://en.wikipedia.org/w/index.php

// --To build from an url with params
//  url("https://en.wikipedia.org/w/index.php") params ("search" -> "Wikipedia")
//
// Will yield:
// https://en.wikipedia.org/w/index.php?search=Wikipedia
sealed trait Request {
  private[asyncclient] def inner: Req

  def /(path: String): Request

  def params(paramList: Map[String, String]): Request

  def auth(username: String, password: String): Request
}

class AsyncClient(config: BaseHttpClientConfig, name: String) extends BaseAsyncClient {

  def this(config: BaseHttpClientConfig) = this(config, "async-client")

  private val httpClient = Http.configure(_.
    setAllowPoolingConnection(config.allowPoolingConnection).
    setConnectionTimeoutInMs(config.connectionTimeoutInMs).
    setRequestTimeoutInMs(config.requestTimeoutInMs).
    setMaximumConnectionsPerHost(config.maximumConnectionsPerHost).
    setMaximumConnectionsTotal(config.maximumConnectionsTotal).
    setCompressionEnabled(config.compressionEnabled))

  private def setBody[RequestBody](req: Req, value: RequestBody,
                                   handler: RequestHandler[RequestBody]): Req = {
    req.setHeader("content-type", buildContentTypeHeaderValue(handler)).
      setBody(handler.from(value))
  }

  private def buildContentTypeHeaderValue[RequestBody](handler: RequestHandler[RequestBody]) = {
    handler.charset match {
      case Some(charset) => s"${handler.contentType}; charset=$charset"
      case None => handler.contentType
    }
  }

  def get[Response](request: Request)(implicit handler: ResponseHandler[Response],
                                      ec: ExecutionContext): Future[Response] = {
    httpClient(request.inner > (v => handler.to(v))).
      recover(transformException("GET", request))
  }

  def post[RequestBody, Response](request: Request, value: RequestBody)(implicit requestHandler: RequestHandler[RequestBody],
                                                                        responseHandler: ResponseHandler[Response],
                                                                        ec: ExecutionContext): Future[Response] = {
    httpClient(setBody(request.inner.POST, value, requestHandler) > (v => responseHandler.to(v))).
      recover(transformException("POST", request))
  }

  def put[RequestBody, Response](request: Request, value: RequestBody)(implicit requestHandler: RequestHandler[RequestBody],
                                                                       responseHandler: ResponseHandler[Response],
                                                                       ec: ExecutionContext): Future[Response] = {
    httpClient(setBody(request.inner.PUT, value, requestHandler) > (v => responseHandler.to(v))).
      recover(transformException("PUT", request))
  }

  def delete[Response](request: Request)(implicit handler: ResponseHandler[Response],
                                         ec: ExecutionContext): Future[Response] = {
    httpClient(request.inner.DELETE > (v => handler.to(v))).
      recover(transformException("DELETE", request))
  }

  //Reformat ExecutionException to add the context of the HTTP call in the Exception message.
  private def transformException(method: String, request: Request): PartialFunction[Throwable, Nothing] = {
    case e: ExecutionException => {
      val baseMessage = s"Exception while executing $method $request."
      val message = if (e.getMessage.isEmpty) baseMessage else s"$baseMessage Original message: ${e.getMessage}."
      throw new ExecutionException(message, e.getCause)
    }
  }
}

object AsyncClient {

  private case class RequestImpl(inner: Req) extends Request {
    def /(path: String) = RequestImpl(inner / path)

    def params(paramList: Map[String, String]) = RequestImpl(inner <<? paramList)

    def auth(username: String, password: String) = RequestImpl(inner as (username, password))

    override def toString(): String = inner.toRequest.getUrl
  }

  def host(url: String): Request = RequestImpl(dispatch.host(url))

  def host(url: String, port: Int): Request = RequestImpl(dispatch.host(url, port))

  def secureHost(url: String): Request = RequestImpl(dispatch.host(url).secure)

  def secureHost(url: String, port: Int): Request = RequestImpl(dispatch.host(url, port).secure)

  def url(url: String): Request = RequestImpl(dispatch.url(url))

  val charsetRegex = "(?<=charset=)[^;]*".r

  def extractCharset(response: Response, default: String = "UTF-8"): String = {
    charsetRegex.findFirstMatchIn(response.getContentType).map(_.group(0)).filter(_.nonEmpty).getOrElse(default)
  }
}

trait RequestHandler[RequestBody] {
  def contentType: String

  def charset: Option[String]

  def from(value: RequestBody): Array[Byte]
}

trait ResponseHandler[Response] {
  def to(value: client.Response): Response
}
