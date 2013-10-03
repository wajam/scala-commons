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

  sealed trait Request {
    private[asyncclient] def inner: Req

    def /(path: String)

    def params(paramList: Map[String, String])
  }

  private case class RequestImpl(inner: Req) extends Request {
    def /(path: String) = RequestImpl(inner / path)

    def params(paramList: Map[String, String]) = RequestImpl(inner <<? paramList)
  }

  implicit def stringToRequest(myUrl: String): Request = RequestImpl(url(myUrl))

  def host(url: String): Request = RequestImpl(dispatch.host(url))

  def host(url: String, port: Int): Request = RequestImpl(dispatch.host(url, port))
}

trait RequestHandler[RequestBody] {
  def contentType: String

  def from(value: RequestBody): Array[Byte]
}

trait ResponseHandler[Response] {
  def to(value: client.Response): Response
}
