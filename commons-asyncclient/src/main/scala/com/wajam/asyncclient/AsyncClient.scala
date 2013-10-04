package com.wajam.asyncclient

import scala.concurrent.{ExecutionContext, Future}
import dispatch._
import com.ning.http.client
import java.util.concurrent.Executors
import scala.language.implicitConversions

class AsyncClient(config: BaseHttpClientConfig) {

  import AsyncClient.Request

  private implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(config.threadPoolSize))

  protected val httpClient = Http.configure(_.
    setAllowPoolingConnection(config.allowPoolingConnection).
    setConnectionTimeoutInMs(config.connectionTimeoutInMs).
    setRequestTimeoutInMs(config.requestTimeoutInMs).
    setMaximumConnectionsPerHost(config.maximumConnectionsPerHost).
    setMaximumConnectionsTotal(config.maximumConnectionsTotal)
  )

  private def setBody[RequestBody](req: Req, value: RequestBody,
                                   handler: RequestHandler[RequestBody]): Req = {
    req.setHeader("content-type", handler.contentType).setBody(handler.from(value))
  }

  def get[Response](request: Request)
                   (implicit handler: ResponseHandler[Response]): Future[Response] = {
    httpClient(request.inner > (v => handler.to(v)))
  }

  def post[RequestBody, Response](request: Request, value: RequestBody)
                                 (implicit requestHandler: RequestHandler[RequestBody],
                                  responseHandler: ResponseHandler[Response]): Future[Response] = {
    httpClient(setBody(request.inner.POST, value, requestHandler) > (v => responseHandler.to(v)))
  }

  def put[RequestBody, Response](request: Request, value: RequestBody)
                                (implicit requestHandler: RequestHandler[RequestBody],
                                 responseHandler: ResponseHandler[Response]): Future[Response] = {
    httpClient(setBody(request.inner.PUT, value, requestHandler) > (v => responseHandler.to(v)))
  }

  def delete[Response](request: Request)
                      (implicit handler: ResponseHandler[Response]): Future[Response] = {
    httpClient(request.inner.DELETE > (v => handler.to(v)))
  }
}

object AsyncClient {

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
  }

  private case class RequestImpl(inner: Req) extends Request {
    def /(path: String) = RequestImpl(inner / path)

    def params(paramList: Map[String, String]) = RequestImpl(inner <<? paramList)

    override def toString(): String = inner.toRequest.getUrl
  }

  def host(url: String): Request = RequestImpl(dispatch.host(url))

  def host(url: String, port: Int): Request = RequestImpl(dispatch.host(url, port))

  def secureHost(url: String): Request = RequestImpl(dispatch.host(url).secure)

  def secureHost(url: String, port: Int): Request = RequestImpl(dispatch.host(url, port).secure)

  def url(url: String): Request = RequestImpl(dispatch.url(url))
}

trait RequestHandler[RequestBody] {
  def contentType: String

  def from(value: RequestBody): Array[Byte]
}

trait ResponseHandler[Response] {
  def to(value: client.Response): Response
}
