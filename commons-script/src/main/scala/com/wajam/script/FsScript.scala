package com.wajam.script

import java.io.{ FileWriter, File }

trait FsScript { this: Script =>

  val workingDir: File = new File(".")

  def prefixPath(name: String) = workingDir.toString + s"/$name"

  def file(name: String) = new File(prefixPath(name))

  def writer(name: String) = new FileWriter(file(name))

}
