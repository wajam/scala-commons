package com.wajam.gearman

import scala.concurrent.Future

trait GearmanClient {

  def executeJob(jobName: String, data: Map[String, Any]): Future[Any]

  def enqueueJob(jobName: String, data: Map[String, Any]): Future[Any]

  def shutdown(): Unit

}
