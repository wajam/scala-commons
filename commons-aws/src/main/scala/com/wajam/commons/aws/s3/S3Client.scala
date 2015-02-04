package com.wajam.commons.aws.s3

import java.io.{ File, FileInputStream, InputStream }
import scala.concurrent.{ ExecutionContext, Future, Promise }

import com.amazonaws.AmazonServiceException
import com.amazonaws.event.{ ProgressEvent, ProgressEventType, ProgressListener }
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.transfer.{ Transfer, TransferManager }

class S3Client(id: String, client: TransferManager) extends S3 {

  def bucket(bucket: String): Bucket = new S3Bucket(client, bucket)

  def close(): Unit = client.shutdownNow()

}

class S3Bucket(client: TransferManager, bucket: String) extends Bucket {

  def get(key: String, file: File)(implicit ec: ExecutionContext): Future[Unit] = {
    transfer(key, client.download(bucket, key, file))
  }

  def get(key: String)(implicit ec: ExecutionContext): Future[InputStream] = {
    val file = File.createTempFile("S3-", ".tmp")
    get(key, file).map(_ => new FileInputStream(file))
  }

  def put(key: String, file: File)(implicit ec: ExecutionContext): Future[Unit] = transfer(key, client.upload(bucket, key, file))

  def put(key: String, stream: InputStream, contentType: Option[String] = None, contentLength: Option[Long] = None)(implicit ec: ExecutionContext): Future[Unit] = {
    val metadata = new ObjectMetadata
    contentType.foreach(metadata.setContentType)
    contentLength.foreach(metadata.setContentLength)
    transfer(key, client.upload(bucket, key, stream, metadata))
  }

  private def wrapFuture(register: ProgressListener => Unit, unregister: ProgressListener => Unit, transfer: Transfer)(implicit ec: ExecutionContext): Future[Unit] = {
    val p = Promise[Unit]()
    val listener = new ProgressListener {
      override def progressChanged(event: ProgressEvent): Unit = {
        event.getEventType match {
          case ProgressEventType.TRANSFER_COMPLETED_EVENT => p.trySuccess()
          case ProgressEventType.TRANSFER_CANCELED_EVENT => p.tryFailure(new S3FailedException("Request Cancelled"))
          case ProgressEventType.TRANSFER_FAILED_EVENT => p.tryFailure(transfer.waitForException)
          case _ => ()
        }
      }
    }

    val future = p.future
    // Register/unregisters listeners
    register(listener)
    future.onComplete(_ => unregister(listener))
    future
  }

  private def transfer(key: String, body: => Transfer)(implicit ec: ExecutionContext): Future[Unit] = {
    try {
      val transfer = body
      wrapFuture(l => transfer.addProgressListener(l), l => transfer.removeProgressListener(l), transfer)
    } catch {
      case e: AmazonServiceException if e.getStatusCode == 404 => Future.failed(new S3NotFoundException(bucket, key))
      case e: Throwable => Future.failed(new S3FailedException(e))
    }
  }

}

