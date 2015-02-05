package com.wajam.commons.aws.s3

import java.io.{ ByteArrayInputStream, File, InputStream }
import scala.concurrent.{ ExecutionContext, Future }

import com.amazonaws.auth.{ AWSCredentials, BasicAWSCredentials }
import com.amazonaws.services.s3.transfer.TransferManager

trait S3 {

  def bucket(bucket: String): Bucket

  def close: Unit

}

object S3 {

  def apply(client: TransferManager): S3 = new S3Client(client.toString, client)

  def apply(credentials: AWSCredentials): S3 = new S3Client(
    credentials.getAWSAccessKeyId,
    new TransferManager(credentials))

  def apply(accessKey: String, secretKey: String): S3 = apply(new BasicAWSCredentials(accessKey, secretKey))

}

trait Bucket {

  def get(key: String, file: File)(implicit ec: ExecutionContext): Future[Unit]

  def get(key: String)(implicit ec: ExecutionContext): Future[InputStream]

  def put(key: String, file: File)(implicit ec: ExecutionContext): Future[Unit]

  def put(key: String, stream: InputStream, contentType: Option[String] = None, contentLength: Option[Long] = None)(implicit ec: ExecutionContext): Future[Unit]

  def put(key: String, bytes: Array[Byte], contentType: Option[String])(implicit ec: ExecutionContext): Future[Unit] = put(key, new ByteArrayInputStream(bytes), contentType)

  def put(key: String, str: String)(implicit ec: ExecutionContext): Future[Unit] = put(key, str.getBytes("UTF8"), Some("text/plain"))

}