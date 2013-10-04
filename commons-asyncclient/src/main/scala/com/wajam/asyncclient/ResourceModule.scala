package com.wajam.asyncclient

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import com.yammer.metrics.scala.{Timer, Instrumented}
import java.util.concurrent.Executors

trait ResourceModule[RequestBody, ResponseMessage <: ConvertableResponse[TypedResponse], TypedResponse[_]] extends Instrumented {

  protected def client: BaseAsyncClient

  implicit protected def requestHandler: RequestHandler[RequestBody]

  implicit protected def responseHandler: ResponseHandler[ResponseMessage]

  implicit protected def decomposer: Decomposer[RequestBody]

  // Execution context used to stop timers. I guess that's enough threads
  implicit private val ec = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  private def timeAction[T](timer: Timer)(action: => Future[T]): Future[T] = {
    val context = timer.timerContext()
    action onComplete {
      case _ => context.stop()
    }
    action
  }

  trait Resource {
    protected def url: String

    protected def name: String
  }

  trait CreatableResource[Value] extends Resource {
    lazy val createMeter = metrics.timer(s"$name-creates")

    def create(value: Value)(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      timeAction(createMeter) {
        client.post(AsyncClient.url(url), decomposer.decompose(value)).map(_.as[Value])
      }
    }
  }

  trait ApplicableResource[Key, R <: Resource] extends Resource {
    def apply(key: Key): R
  }

  trait GettableResource[Value] extends Resource {
    lazy val getMeter = metrics.timer(s"$name-gets")

    def get()(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      timeAction(getMeter) {
        client.get(AsyncClient.url(url)).map(_.as[Value])
      }
    }
  }

  trait UpdatableResource[Value] extends Resource {
    lazy val updateMeter = metrics.timer(s"$name-updates")

    def update(value: Value)(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      timeAction(updateMeter) {
        client.put(AsyncClient.url(url), decomposer.decompose(value)).map(_.as[Value])
      }
    }
  }

  trait DeletableResource[Value] extends Resource {
    lazy val deleteMeter = metrics.timer(s"$name-deletes")

    def delete()(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      timeAction(deleteMeter) {
        client.delete(AsyncClient.url(url)).map(_.as[Value])
      }
    }
  }

}

trait Decomposer[I] {
  def decompose[Value](value: Value): I
}

