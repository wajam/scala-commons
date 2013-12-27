package com.wajam.gearman.impl

import java.net.InetSocketAddress
import scala.concurrent.{ExecutionContext, Future, Promise}
import com.wajam.commons.Logging
import com.yammer.metrics.scala.Instrumented
import com.yammer.metrics.core.TimerContext
import org.gearman.core.GearmanConstants
import org.gearman.{GearmanClient => JavaGearmanClient, GearmanBackgroundJob, GearmanJobResult, GearmanJob, Gearman}
import org.gearman.GearmanClient.{SubmitCallbackResult, GearmanSubmitHandler}
import org.gearman.GearmanJob.Priority
import com.wajam.gearman.exception.{JobExecutionException, JobSubmissionException}
import scala.util.{Failure, Success}
import com.wajam.gearman.WajamGearmanClient
import com.wajam.gearman.utils.GearmanJSON
import com.wajam.tracing.Traced


class AsyncWajamGearmanClient(serverAddress: Seq[String])(implicit val ec: ExecutionContext) extends WajamGearmanClient with Logging with Instrumented with Traced {

  private val jobQueuedTimer = metrics.timer("job-queued", "calls")
  private val jobCompletedTimer = metrics.timer("job-completed", "calls")

  private val metricSubmitJob = tracedTimer("gearman-submit")

  private val gearmanService: Gearman = new Gearman()

  private val gearmanClient: JavaGearmanClient = gearmanService.createGearmanClient()

  //Initialize gearman client connection with all addresses
  serverAddress.foreach(server => {
    if (!server.isEmpty) {
      gearmanClient.addServer(new InetSocketAddress(server, GearmanConstants.DEFAULT_PORT))
    }
  })

  //Return a future that complete when the job is completed (success, failure)
  // Or failure when job can't be queued
  override def executeJob(jobName: String, data: Map[String, Any]) = {
    val jobPromised = Promise[Any]()
    sendJob(jobName, data, Priority.NORMAL_PRIORITY, Option(jobPromised)).onComplete {
      case Failure(e) => jobPromised.failure(e)
      case Success(v) => //Do Nothing
    }
    jobPromised.future
  }

  //Return a future that complete when job is queued (success, failure)
  override def enqueueJob(jobName: String, data: Map[String, Any]) = {
    sendJob(jobName, data, Priority.NORMAL_PRIORITY, None)
  }

  //Stop properly the gearman client
  override def shutdown() {
    // When all jobs are done, shutdown gearman client.
    gearmanService.shutdown()
  }

  //Private method that send job to gearman
  // with an Option of jobCompletedPromise: If set, will create a callback on a completion, else it will be a background job
  private def sendJob(jobName: String, data: Map[String, Any], gearmanPriority: Priority, jobCompletedPromise: Option[Promise[Any]]): Future[Any] = {
    val jobEnqueuedPromise = Promise[Any]()

    if (gearmanClient.getServerCount > 0) {
      val jobQueuedTimerContext: Option[TimerContext] = Some(jobQueuedTimer.timerContext())
      var jobCompletedTimerContext: Option[TimerContext] = None

      val handler: GearmanSubmitHandler = new GearmanSubmitHandler() {
        def onComplete(job: GearmanJob, result: SubmitCallbackResult) {
          if (!jobEnqueuedPromise.isCompleted) {
            if (jobQueuedTimerContext.isDefined) {
              jobQueuedTimerContext.get.stop()
            }

            if (result.isSuccessful) {
              jobCompletedTimerContext = Some(jobCompletedTimer.timerContext())
              jobEnqueuedPromise.success(data)
            } else {
              error("Couldn't submit job", result.toString)
              jobEnqueuedPromise.failure(new JobSubmissionException(data, s"Couldn't submit job $jobName ${result.toString}"))
            }
          }
        }
      }

      val job: GearmanJob = jobCompletedPromise match {
        //Create a gearman job with callback
        case Some(promise) =>
          new GearmanJob(jobName, GearmanJSON.encodeAsJson(data), gearmanPriority) {
            protected def onComplete(result: GearmanJobResult) {
              if (jobCompletedTimerContext.isDefined) {
                jobCompletedTimerContext.get.stop()
              }

              if (result.isSuccessful) {
                promise.success(data)
              } else {
                error(s"Error processing the job $jobName ${result.getJobCallbackResult.toString}")
                promise.failure(new JobExecutionException(data, s"Job couldn't be completed properly $jobName ${result.getJobCallbackResult().toString}"))
              }
            }

            def callbackWarning(warning: Array[Byte]) {
            }

            def callbackStatus(numerator: Long, denominator: Long) {
            }

            def callbackData(data: Array[Byte]) {
            }
          }
        //Create a background gearman job (no callback)
        case None => new GearmanBackgroundJob(jobName, GearmanJSON.encodeAsJson(data), gearmanPriority)
      }

      try {
        metricSubmitJob.time {
          gearmanClient.submitJob(job, handler)
        }
      } catch {
        //UnresolvedAddressException
        case e: Exception =>
          jobEnqueuedPromise.failure(new JobSubmissionException(data, s"Couldn't submit job $jobName ${e.getMessage}"))
          if (jobQueuedTimerContext.isDefined) {
            jobQueuedTimerContext.get.stop()
          }
      }
    } else {
      error("No Gearman servers specified. Jobs can't be sent.")
      jobEnqueuedPromise.failure(new JobSubmissionException(data, s"Couldn't submit job $jobName, no servers specified."))
    }

    jobEnqueuedPromise.future
  }

}

