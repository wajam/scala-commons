package com.wajam.commons

import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.mock.MockitoSugar
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.io._
import java.util.zip.GZIPOutputStream

@RunWith(classOf[JUnitRunner])
class TestFileUtils extends FunSuite with MockitoSugar with Matchers with BeforeAndAfter {

  val f = new File("test")

  after {
    f.delete()
  }

  test("Should write and read data") {
    val data = "SOME_DATA_TOA_FILE"

    val writeState = FileUtils.writeData(f, data)
    val read = FileUtils.readLines(f) { l => l.next() }

    writeState should be(true)
    read shouldEqual data
  }

  test("Should write and read lines data") {
    val lines = "SOME_DATA_TOA_FILE\rOTHER_LINE"

    val writeState = FileUtils.writeData(f, lines)
    val read = FileUtils.readLines(f) { l => l.toList }

    writeState should be(true)
    read shouldEqual lines.split("\r")
  }

  test("Should write and read lines") {
    val lines = List("SOME_DATA_TOA_FILE", "OTHER_LINE", "ANOTHER line with random stuff")

    val writeState = FileUtils.writeLines(f, lines)
    val read = FileUtils.readLines(f) { l => l.toList }

    writeState should be(true)
    read shouldEqual lines
  }

  test("Should read lines from Gzip") {
    val lines = List("SOME_DATA_TOA_FILE", "OTHER_LINE", "ANOTHER line with random stuff")

    val writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f)), "UTF-8"))
    lines.foreach { line =>
      writer.write(line)
      writer.newLine()
    }

    writer.flush()
    writer.close()

    val read = FileUtils.readLinesGzip(f) { l => l.toList }

    read shouldEqual lines
  }

}
