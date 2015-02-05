package com.wajam.commons.aws.s3

class S3FailedException(msg: Option[String] = None, ex: Option[Throwable] = None) extends Exception(msg.getOrElse(""), ex.orNull) {
  def this(ex: Throwable) = this(None, Some(ex))
  def this(msg: String) = this(Some(msg))
}

case class S3NotFoundException(bucket: String, key: String) extends S3FailedException(Some(s"S3 resource not found: $bucket/$key"))
