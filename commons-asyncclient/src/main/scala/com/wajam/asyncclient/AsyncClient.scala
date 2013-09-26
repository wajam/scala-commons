package com.wajam.asyncclient

import scala.concurrent.{ExecutionContext, Future}
import dispatch._
import com.ning.http.client
import java.util.concurrent.Executors

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

  def get[Response, O](myUrl: String)
                      (as: Response => O)
                      (implicit handler: ResponseHandler[Response]): Future[O] = {
    httpClient(url(myUrl) > (v => as(handler.to(v))))
  }

  def post[RequestBody, Response, O](myUrl: String, value: RequestBody)
                                    (as: Response => O)
                                    (implicit requestHandler: RequestHandler[RequestBody],
                                     responseHandler: ResponseHandler[Response]): Future[O] = {
    httpClient(setBody(url(myUrl).POST, value, requestHandler) > (v => as(responseHandler.to(v))))
  }

  def put[RequestBody, Response, O](myUrl: String, value: RequestBody)
                                   (as: Response => O)
                                   (implicit requestHandler: RequestHandler[RequestBody],
                                    responseHandler: ResponseHandler[Response]): Future[O] = {
    httpClient(setBody(url(myUrl).PUT, value, requestHandler) > (v => as(responseHandler.to(v))))
  }

  def delete[Response, O](myUrl: String)
                         (as: Response => O)
                         (implicit handler: ResponseHandler[Response]): Future[O] = {
    httpClient(url(myUrl).DELETE > (v => as(handler.to(v))))
  }
}

trait RequestHandler[RequestBody] {
  def contentType: String

  def from(value: RequestBody): Array[Byte]
}

trait ResponseHandler[Response] {
  def to(value: client.Response): Response
}
