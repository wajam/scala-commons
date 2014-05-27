package com.wajam.script

import com.wajam.commons.Logging
import com.wajam.script.Script.Executor
import scala.collection.mutable.ListBuffer

trait Script extends DelayedInit with Logging {

  protected var steps = scala.collection.mutable.MutableList[ScriptStep]()

  protected def addStep(step: ScriptStep) = {
    steps += step
    step
  }

  implicit class desc2in(desc: String) {

    def in(executor: Executor) = execs(executor)

    def updates(executor: Executor) = execs(executor)

    def extracts(executor: Executor) = execs(executor)

    def execs(executor: Executor): ScriptStep = {
      addStep(new ScriptStep {
        def execute() = executor.execute()

        val description = desc
      })
    }

    def execs[T](f: => T): ScriptStep = execs(new Executor {
      def execute(): Unit = f
    })

    def prints(text: String) = execs(new Executor {
      def execute() {
        println(text)
      }
    })
  }

  def showMenu() {
    initialize()

    def validChoice(choice: String) = !choice.isEmpty && choice.forall(_.isDigit) && choice.toInt < steps.size

    while (true) {
      println("------------------------------------")
      println("Menu: ")

      steps.zipWithIndex.foreach {
        case (step, i) => println(s"  $i) ${step.description}")
      }

      println("  q) Quit")

      print("Choice: ")
      readLine().trim match {
        case "q" =>
          destroy()
          return
        case choice if validChoice(choice) =>
          try {
            steps(choice.toInt).execute()
          } catch {
            case t: Throwable => t.printStackTrace()
          }
        case _ =>
      }
    }
  }

  /*
   * Code taken from Scala's "App" class. Reimplemented to execute menu
   * once the script is loaded
   */
  private val initCode = new ListBuffer[() => Unit]
  private var initialized = false

  override def delayedInit(body: => Unit) {
    initCode += (() => body)
  }

  def initialize() {
    if (!initialized) {
      for (proc <- initCode) proc()
      initialized = true
    }
  }

  def destroy() {
  }

  def main(args: Array[String]) = {
    showMenu()
  }
}

trait ScriptStep {

  val description: String

  def execute()

  def apply() = execute()

}

object Script {

  trait Executor {
    def execute()
  }

}

