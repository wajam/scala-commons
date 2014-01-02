package com.wajam.gearman.exception

case class JobSubmissionException(data: Any, msg: String) extends Exception(msg)