package com.wajam.asyncclient

import scala.concurrent.{ExecutionContext, Future}
import dispatch._
import com.ning.http.client
import java.util.concurrent.Executors
import scala.language.implicitConversions

class AsyncClient(config: BaseHttpClientConfig) {

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

  def get[Response](myUrl: Req)
                   (implicit handler: ResponseHandler[Response]): Future[Response] = {
    httpClient(myUrl > (v => handler.to(v)))
  }

  def typedGet[Response, O](myUrl: Req)
                           (as: Response => O)
                           (implicit handler: ResponseHandler[Response]): Future[O] = {
    httpClient(myUrl > (v => as(handler.to(v))))
  }

  def post[RequestBody, Response](myUrl: Req, value: RequestBody)
                                 (implicit requestHandler: RequestHandler[RequestBody],
                                  responseHandler: ResponseHandler[Response]): Future[Response] = {
    httpClient(setBody(myUrl.POST, value, requestHandler) > (v => responseHandler.to(v)))
  }

  def typedPost[RequestBody, Response, O](myUrl: Req, value: RequestBody)
                                         (as: Response => O)
                                         (implicit requestHandler: RequestHandler[RequestBody],
                                          responseHandler: ResponseHandler[Response]): Future[O] = {
    httpClient(setBody(myUrl.POST, value, requestHandler) > (v => as(responseHandler.to(v))))
  }

  def put[RequestBody, Response](myUrl: Req, value: RequestBody)
                                (implicit requestHandler: RequestHandler[RequestBody],
                                 responseHandler: ResponseHandler[Response]): Future[Response] = {
    httpClient(setBody(myUrl.PUT, value, requestHandler) > (v => responseHandler.to(v)))
  }

  def typedPut[RequestBody, Response, O](myUrl: Req, value: RequestBody)
                                        (as: Response => O)
                                        (implicit requestHandler: RequestHandler[RequestBody],
                                         responseHandler: ResponseHandler[Response]): Future[O] = {
    httpClient(setBody(myUrl.PUT, value, requestHandler) > (v => as(responseHandler.to(v))))
  }

  def delete[Response](myUrl: Req)
                      (implicit handler: ResponseHandler[Response]): Future[Response] = {
    httpClient(myUrl.DELETE > (v => handler.to(v)))
  }

  def typedDelete[Response, O](myUrl: Req)
                              (as: Response => O)
                              (implicit handler: ResponseHandler[Response]): Future[O] = {
    httpClient(myUrl.DELETE > (v => as(handler.to(v))))
  }
}

object AsyncClient {
  implicit def stringToReq(myUrl: String): Req = url(myUrl)
}

trait RequestHandler[RequestBody] {
  def contentType: String

  def from(value: RequestBody): Array[Byte]
}

trait ResponseHandler[Response] {
  def to(value: client.Response): Response
}
