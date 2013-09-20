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

  private def setBody[I](req: Req, value: I, handler: RequestHandler[I]): Req = {
    req.setHeader("content-type", handler.contentType).setBody(handler.from(value))
  }

  def get[Inner, O](myUrl: String)
                   (as: Inner => O)
                   (implicit handler: ResponseHandler[Inner]): Future[O] = {
    httpClient(url(myUrl) > (v => as(handler.to(v))))
  }

  def post[I, Inner, O](myUrl: String, value: I)
                       (as: Inner => O)
                       (implicit requestHandler: RequestHandler[I],
                        responseHandler: ResponseHandler[Inner]): Future[O] = {
    httpClient(setBody(url(myUrl).POST, value, requestHandler) > (v => as(responseHandler.to(v))))
  }

  def put[I, Inner, O](myUrl: String, value: I)
                      (as: Inner => O)
                      (implicit requestHandler: RequestHandler[I],
                       responseHandler: ResponseHandler[Inner]): Future[O] = {
    httpClient(setBody(url(myUrl).PUT, value, requestHandler) > (v => as(responseHandler.to(v))))
  }

  def delete[Inner, O](myUrl: String)
                      (as: Inner => O)
                      (implicit handler: ResponseHandler[Inner]): Future[O] = {
    httpClient(url(myUrl).DELETE > (v => as(handler.to(v))))
  }
}

trait RequestHandler[I] {
  def contentType: String
  def from(value: I): Array[Byte]
}

trait ResponseHandler[O] {
  def to(value: client.Response): O
}
