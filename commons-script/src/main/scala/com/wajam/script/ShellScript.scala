package com.wajam.script

import com.wajam.script.Script.Executor

trait ShellScript { this: Script =>

  object Shell {
    def apply(f: => sys.process.ProcessBuilder) = new Executor {
      def execute(): Unit = shellExec(f)
    }
  }

  def shellExec(process: scala.sys.process.ProcessBuilder) {
    println(process)
    process.! match {
      case 0 => println("Success")
      case _ => println("Error!")
    }
  }

}
