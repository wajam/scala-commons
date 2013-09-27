package com.wajam.asyncclient

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import com.yammer.metrics.scala.Instrumented
import com.yammer.metrics.core.TimerContext
import java.util.concurrent.Executors

trait ResourceModule[RequestBody, ResponseMessage <: ConvertableResponse[TypedResponse], TypedResponse[_]] extends Instrumented {

  protected def client: AsyncClient

  implicit protected def requestHandler: RequestHandler[RequestBody]

  implicit protected def responseHandler: ResponseHandler[ResponseMessage]

  implicit protected def decomposer: Decomposer[RequestBody]

  // Execution context used to stop timers. I guess that's enough threads
  implicit private val ec = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  private def timeAction[T](timer: TimerContext)(action: => Future[T]): Future[T] = {
    action onComplete {
      case _ => timer.stop()
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
      timeAction(createMeter.timerContext()) {
        client.post[RequestBody, ResponseMessage, TypedResponse[Value]](url, decomposer.decompose(value))(_.as[Value])
      }
    }
  }

  trait ApplicableResource[Key, R <: Resource] extends Resource {
    def apply(key: Key): R
  }

  trait GettableResource[Value] extends Resource {
    lazy val getMeter = metrics.timer(s"$name-gets")

    def get()(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      timeAction(getMeter.timerContext()) {
        client.get[ResponseMessage, TypedResponse[Value]](url)(_.as[Value])
      }
    }
  }

  trait UpdatableResource[Value] extends Resource {
    lazy val updateMeter = metrics.timer(s"$name-updates")

    def update(value: Value)(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      timeAction(updateMeter.timerContext()) {
        client.put[RequestBody, ResponseMessage, TypedResponse[Value]](url, decomposer.decompose(value))(_.as[Value])
      }
    }
  }

  trait DeletableResource[Value] extends Resource {
    lazy val deleteMeter = metrics.timer(s"$name-deletes")

    def delete()(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      timeAction(deleteMeter.timerContext()) {
        client.delete[ResponseMessage, TypedResponse[Value]](url)(_.as[Value])
      }
    }
  }

}

trait Decomposer[I] {
  def decompose[Value](value: Value): I
}

