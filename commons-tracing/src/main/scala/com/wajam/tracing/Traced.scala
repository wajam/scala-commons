package com.wajam.tracing

import com.yammer.metrics.core.{TimerContext, MetricName}
import java.util.concurrent.TimeUnit
import com.yammer.metrics.scala.{Instrumented, Timer}
import Annotation.Message

/**
 * The mixin trait for creating a class which is traced with NRV tracing and instrumented with metrics.
 */
trait Traced extends Instrumented {

  def tracedTimer(name: String, scope: String = null,
                  durationUnit: TimeUnit = TimeUnit.MILLISECONDS, rateUnit: TimeUnit = TimeUnit.SECONDS,
                  tracedClass: Option[Class[_]] = None) = {
    new TracedTimer(new Timer(metrics.metricsRegistry.newTimer(tracedClass.getOrElse(getTracedClass),
      name, scope, durationUnit, rateUnit)), new MetricName(tracedClass.getOrElse(getTracedClass), name, scope))
  }

  protected def getTracedClass: Class[_] = getClass
}

/**
 * Wrapper for Yammer metrics Timer
 */
class TracedTimer(val timer: Timer, val name: String, val source: Option[String]) {

  def this(timer: Timer, name: MetricName) {
    this(timer, name.getName, Some(name.getGroup + "." + name.getType))
  }

  /**
   * Runs block, recording its duration, and returns the result of block.
   */
  def time[S](block: => S): S = {
    timer.time {
      Tracer.currentTracer match {
        case Some(tracer) => tracer.time(name, source) { block }
        case _ => block
      }
    }
  }

  /**
   * Adds a recorded duration.
   */
  def update(duration: Long, unit: TimeUnit, extra: Map[String, String] = Map()) {
    Tracer.currentTracer.foreach(_.record(Message(content(extra), source), Some(unit.toMillis(duration))))
    timer.update(duration, unit)
  }

  def timerContext(): TracedTimerContext = {
    new TracedTimerContext(timer.timerContext(), Tracer.currentTracer)
  }

  class TracedTimerContext(timerContext: TimerContext, optTracer: Option[Tracer]) {

    private val startTime: Long = optTracer.map(_.currentTimeGenerator.currentTime).getOrElse(0)

    def stop(extra: Map[String, String] = Map()): Option[Long] = {
      timerContext.stop()

      optTracer.map { tracer =>
        val endTime: Long = tracer.currentTimeGenerator.currentTime
        val totalTime = endTime - startTime
        tracer.record(Message(content(extra), source), Some(totalTime))
        totalTime
      }
    }
  }

  def content(extra: Map[String, String]) = {
    if (extra.isEmpty) {
      name
    } else {
      s"$name ${extra.map { case (k, v) => s"$k=$v"}.mkString("[", ",", "]")}"
    }
  }
}
