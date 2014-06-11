package com.wajam.gearman

import scala.concurrent.{ ExecutionContext, Future }

trait GearmanClient {

  def executeJob(jobName: String, data: Map[String, Any])(implicit ec: ExecutionContext): Future[Any]

  def enqueueJob(jobName: String, data: Map[String, Any])(implicit ec: ExecutionContext): Future[Any]

  def shutdown(): Unit

}
