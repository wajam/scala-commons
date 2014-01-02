package com.wajam.gearman.exception

case class JobExecutionException(data: Any, msg: String) extends Exception(msg)