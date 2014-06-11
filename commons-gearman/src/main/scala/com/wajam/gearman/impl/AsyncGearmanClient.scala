package com.wajam.gearman.impl

import java.net.InetSocketAddress
import scala.concurrent.{ ExecutionContext, Future, Promise }
import com.wajam.commons.Logging
import org.gearman.core.GearmanConstants
import org.gearman.{ GearmanBackgroundJob, GearmanJobResult, GearmanJob, Gearman }
import org.gearman.GearmanClient.{ SubmitCallbackResult, GearmanSubmitHandler }
import org.gearman.GearmanJob.Priority
import com.wajam.gearman.exception.{ JobExecutionException, JobSubmissionException }
import scala.util.{ Failure, Success }
import com.wajam.gearman.GearmanClient
import com.wajam.gearman.utils.GearmanJson
import com.wajam.tracing.Traced

class AsyncGearmanClient(serverAddress: Seq[String]) extends GearmanClient with Logging with Traced {

  private val jobQueuedTimer = tracedTimer("gearman-submit", "gearman-submit")
  private val jobCompletedTimer = tracedTimer("gearman-completed", "gearman-completed")

  private val gearmanService = new Gearman()

  private val javaGearmanClient = gearmanService.createGearmanClient()

  //Initialize gearman client connection with all addresses
  serverAddress.foreach(server => {
    if (!server.isEmpty) {
      javaGearmanClient.addServer(new InetSocketAddress(server, GearmanConstants.DEFAULT_PORT))
    }
  })

  //Return a future that complete when the job is completed (success, failure)
  // Or failure when job can't be queued
  override def executeJob(jobName: String, data: Map[String, Any])(implicit ec: ExecutionContext) = {
    val jobCompletedPromise = Promise[Any]()
    var jobCompletedTimerContext: Option[jobCompletedTimer.TracedTimerContext] = None
    sendJob(jobName, data, Priority.NORMAL_PRIORITY, Option(jobCompletedPromise)).onComplete {
      case Failure(e) => jobCompletedPromise.failure(e)
      case Success(v) => {
        // The job has been submitted successfully. Starting the completion timer to track how long it takes to
        // complete the job after being queued for processing.
        jobCompletedTimerContext = Some(jobCompletedTimer.timerContext())
      }
    }
    val jobCompletedFuture = jobCompletedPromise.future
    jobCompletedFuture.onComplete(_ => jobCompletedTimerContext.foreach(_.stop()))
    jobCompletedFuture
  }

  //Return a future that complete when job is queued (success, failure)
  override def enqueueJob(jobName: String, data: Map[String, Any])(implicit ec: ExecutionContext) = {
    sendJob(jobName, data, Priority.NORMAL_PRIORITY, None)
  }

  //Stop properly the Gearman client
  override def shutdown() {
    // When all jobs are done, shutdown Gearman client.
    gearmanService.shutdown()
  }

  //Private method that send job to Gearman
  // with an Option of jobCompletedPromise: If set, will create a callback on a completion, else it will be a background job
  private def sendJob(jobName: String, data: Map[String, Any], gearmanPriority: Priority, jobCompletedPromise: Option[Promise[Any]])(implicit ec: ExecutionContext): Future[Any] = {
    val jobEnqueuePromise = Promise[Any]()
    val jobEnqueueTimerContext = jobQueuedTimer.timerContext()

    if (javaGearmanClient.getServerCount > 0) {

      val handler: GearmanSubmitHandler = new GearmanSubmitHandler() {
        def onComplete(job: GearmanJob, result: SubmitCallbackResult) {
          if (!jobEnqueuePromise.isCompleted) {
            if (result.isSuccessful) {
              jobEnqueuePromise.success(data)
            } else {
              error("Couldn't submit job", result.toString)
              jobEnqueuePromise.failure(new JobSubmissionException(data, s"Couldn't submit job $jobName ${result.toString}"))
            }
          }
        }
      }

      val job: GearmanJob = jobCompletedPromise match {
        //Create a gearman job with callback
        case Some(promise) =>
          new GearmanJob(jobName, GearmanJson.encodeAsJson(data), gearmanPriority) {
            protected def onComplete(result: GearmanJobResult) {
              if (result.isSuccessful) {
                promise.success(data)
              } else {
                error(s"Error processing the job $jobName ${result.getJobCallbackResult.toString}")
                promise.failure(new JobExecutionException(data, s"Job couldn't be completed properly $jobName ${result.getJobCallbackResult.toString}"))
              }
            }

            def callbackWarning(warning: Array[Byte]) {}

            def callbackStatus(numerator: Long, denominator: Long) {}

            def callbackData(data: Array[Byte]) {}
          }
        //Create a background gearman job (no callback)
        case None => new GearmanBackgroundJob(jobName, GearmanJson.encodeAsJson(data), gearmanPriority)
      }

      try {
        javaGearmanClient.submitJob(job, handler)
      } catch {
        //UnresolvedAddressException
        case e: Exception => jobEnqueuePromise.failure(
          new JobSubmissionException(data, s"Couldn't submit job $jobName ${e.getMessage}"))
      }
    } else {
      error("No Gearman servers specified. Jobs can't be sent.")
      jobEnqueuePromise.failure(new JobSubmissionException(data, s"Couldn't submit job $jobName, no servers specified."))
    }

    val jobEnqueueFuture = jobEnqueuePromise.future
    jobEnqueueFuture.onComplete(_ => jobEnqueueTimerContext.stop())
    jobEnqueueFuture
  }

}

