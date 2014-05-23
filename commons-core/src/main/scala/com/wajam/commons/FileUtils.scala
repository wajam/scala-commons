package com.wajam.commons

import java.io.{FileInputStream, BufferedInputStream, File}
import scala.io.Source._
import com.wajam.commons.Closable._
import org.apache.commons.io.{FileUtils => ApacheFileUtils}
import scala.io.Source
import java.util.zip.GZIPInputStream

object FileUtils extends Logging {

  def writeData(fileName: String, data: String, encoding: String = "UTF-8"): Boolean = {
    writeSafe(fileName, encoding) { (file) =>
      ApacheFileUtils.write(file, data, encoding, false)
    }
  }

  def writeLines(fileName: String, lines: Iterable[String], encoding: String = "UTF-8"): Boolean = {
    writeSafe(fileName, encoding) { (file) =>
      import collection.JavaConversions._
      ApacheFileUtils.writeLines(file, encoding, lines, false)
    }
  }

  /**
   * Method that write into a temporary files, then moves it to replace to original for 'safe' write
   * @param fileName path of the file to write to
   * @param encoding file encoding; default UTF-8
   * @param writeFn wrapped function taking File as a parameter that actually writes to the file
   * @return true if success, false otherwise
   */
  private def writeSafe(fileName: String, encoding: String = "UTF-8")(writeFn: (File) => Unit): Boolean = {
    val tmpFileName = s"$fileName.tmp"
    try {
      val tmpFile = new File(tmpFileName)
      writeFn(tmpFile)
      val status = tmpFile renameTo new File(fileName)
      if (!status) error(s"The impossible happened: unable to rename $tmpFile to $fileName")
      status
    } catch {
      case e: Exception => {
        error(e.getMessage)
        false
      }
    }
  }

  def readLines[T](fileName: String)(block: (Iterator[String]) => T): Option[T] = {
    val file = new File(fileName)
    if (file.exists()) {
      val source = fromFile(file)
      Some(readFromSource(source)(block))
    } else None
  }

  def readLinesGzip[T](fileName: String)(block: (Iterator[String]) => T): Option[T] = {
    val file = new File(fileName)
    if (file.exists()) {
      val source = Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file))))
      Some(readFromSource(source)(block))
    } else None
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

}
