package com.wajam.asyncclient

import scala.concurrent.{ExecutionContext, Future}
import dispatch._
import com.ning.http.client
import java.util.concurrent.Executors

trait BaseClient[Request, InnerResponse] {

  private implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(config.threadPoolSize))

  protected def config: BaseHttpClientConfig

  protected def contentType: String

  protected val httpClient = Http.configure(_.
    setAllowPoolingConnection(config.allowPoolingConnection).
    setConnectionTimeoutInMs(config.connectionTimeoutInMs).
    setRequestTimeoutInMs(config.requestTimeoutInMs).
    setMaximumConnectionsPerHost(config.maximumConnectionsPerHost).
    setMaximumConnectionsTotal(config.maximumConnectionsTotal)
  )

  private def setBody(req: Req, value: Request): Req = {
    req.setHeader("content-type", contentType).setBody(from(value))
  }

  def get[A](myUrl: String)(as: InnerResponse => A): Future[A] = {
    httpClient(url(myUrl) > (v => as(to(v))))
  }

  def post[A](myUrl: String, value: Request)(as: InnerResponse => A): Future[A] = {
    httpClient(setBody(url(myUrl).POST, value) > (v => as(to(v))))
  }

  def put[A](myUrl: String, value: Request)(as: InnerResponse => A): Future[A] = {
    httpClient(setBody(url(myUrl).PUT, value) > (v => as(to(v))))
  }

  def delete[A](myUrl: String)(as: InnerResponse => A): Future[A] = {
    httpClient(url(myUrl).DELETE > (v => as(to(v))))
  }

  protected def to(value: client.Response): InnerResponse

  protected def from(value: Request): Array[Byte]

}
