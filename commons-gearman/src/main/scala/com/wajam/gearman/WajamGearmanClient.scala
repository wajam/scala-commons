package com.wajam.gearman

import scala.concurrent.Future

trait WajamGearmanClient {

  def executeJob(jobName: String, data: Map[String, Any]): Future[Any]

  def enqueueJob(jobName: String, data: Map[String, Any]): Future[Any]

  def shutdown(): Unit

}
