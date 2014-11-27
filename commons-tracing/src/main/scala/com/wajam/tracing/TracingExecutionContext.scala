package com.wajam.tracing

import scala.concurrent.ExecutionContext

import com.wajam.commons.Logging

/**
 * Execution context decorator that saves the current tracing context when created and restore it every time
 * it is executed.
 */
class TracingExecutionContext(ec: ExecutionContext) extends ExecutionContext with Logging {

  val tracer = Tracer.currentTracer.get
  val tracingContext = tracer.currentContext

  def execute(runnable: Runnable) = {
    ec.execute(new Runnable {
      def run() {
        tracer.trace(tracingContext) {
          runnable.run()
        }
      }
    })
  }

  def reportFailure(t: Throwable) = ec.reportFailure(t)
}

