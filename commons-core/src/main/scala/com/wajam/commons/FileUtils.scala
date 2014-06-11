package com.wajam.commons

import java.io._
import scala.io.Source._
import com.wajam.commons.Closable._
import scala.io.{ Codec, Source }
import java.util.zip.GZIPInputStream
import java.security.AccessController
import sun.security.action.GetPropertyAction

object FileUtils extends Logging {

  implicit private val codec = Codec.UTF8

  def writeData(file: File, data: String): Boolean = {
    writeSafe(file) { writer =>
      writer.write(data)
    }
  }

  def writeLines(file: File, lines: Iterable[String]): Boolean = {
    writeSafe(file) { writer =>
      lines.foreach { line =>
        writer.write(line)
        writer.newLine()
      }
    }
  }

  /**
   * Method that write into a temporary files, then moves it to replace to original for 'safe' write
   * @param file file to write to
   * @param writeFn wrapped function taking BufferedWriter as a parameter that actually writes to the file
   * @return true if success, false otherwise
   */
  private def writeSafe(file: File)(writeFn: (BufferedWriter) => Unit): Boolean = {
    val tmpFileName = s"${file.getPath}.tmp"
    try {
      val tmpFile = new File(tmpFileName)

      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpFileName), "UTF-8"))
      try {
        writeFn(writer)
        writer.flush()
      } finally {
        writer.close()
      }

      val status = tmpFile renameTo file
      if (!status) error(s"Unable to rename $tmpFile to $file")
      status
    } catch {
      case e: Exception => {
        error(s"Error when trying to write to ${file.getPath}.", e)
        false
      }
    }
  }

  def readLines[T](file: File)(readFn: (Iterator[String]) => T): T = {
    val source = fromFile(file)
    readFromSource(source)(readFn)
  }

  def readLinesGzip[T](file: File)(readFn: (Iterator[String]) => T): T = {
    val source = fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file))))
    readFromSource(source)(readFn)
  }

  private def readFromSource[T](s: Source)(block: (Iterator[String]) => T): T = {
    val iteratorWithClosable = new Iterator[String] with Closable {
      val lines = s.getLines()

      override def hasNext: Boolean = lines.hasNext

      override def close(): Unit = s.close()

      override def next(): String = lines.next()
    }

    using(iteratorWithClosable)(block)
  }

  // Code extracted from File.Java to determine the tmpdir on the system.
  def getTmpDirectory: File = new File(AccessController.doPrivileged(new GetPropertyAction("java.io.tmpdir")))

}
