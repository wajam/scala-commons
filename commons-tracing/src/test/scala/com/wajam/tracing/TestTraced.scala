package com.wajam.tracing

import java.util.concurrent.TimeUnit

import com.yammer.metrics.Metrics
import com.yammer.metrics.scala.Timer
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfter, FunSuite }

import com.wajam.commons.ControlableCurrentTime
import com.wajam.tracing.Annotation.Message

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class TestTraced extends FunSuite with BeforeAndAfter with MockitoSugar {

  val mockRecorder: TraceRecorder = mock[TraceRecorder]
  val time = new ControlableCurrentTime {}
  val tracer = new Tracer(mockRecorder, time)

  before {
    reset(mockRecorder)
  }

  class TracedObject extends Traced {
    val timer = tracedTimer("myTimer")
  }

  test("Should returns a TracedTimer") {

    val traced = new TracedObject()

    traced.timer.name should be("myTimer")
    traced.timer.source.get should be("com.wajam.tracing.TracedObject")
    traced.timer.timer should not be (null)
  }

  test("TracedTimer.time should trace and update timer within a tracing context") {

    val yammerTimer = new Timer(Metrics.defaultRegistry().newTimer(classOf[TracedTimer], "timer"))
    yammerTimer.clear()
    yammerTimer.min should be(0.0)

    val tracedTimer = new TracedTimer(yammerTimer, "myName", Some("mySource"))

    val message = Message("myName", Some("mySource"))
    var context: Option[TraceContext] = None
    val duration = 250

    var called = false
    tracer.trace() {
      context = tracer.currentContext
      called = tracedTimer.time {
        Thread.sleep(duration) // Delay for yammer timer
        time.currentTime += duration
        true
      }
    }

    called should be(true)
    yammerTimer.min should be > (0.0)
    verify(mockRecorder).record(Record(context.get, time.currentTime, message, Some(duration)))
  }

  test("TracedTimer.time should update timer without error outside a tracing context") {

    val yammerTimer = new Timer(Metrics.defaultRegistry().newTimer(classOf[TracedTimer], "timer"))
    yammerTimer.clear()
    yammerTimer.min should be(0.0)

    val tracedTimer = new TracedTimer(yammerTimer, "myName", Some("mySource"))

    val duration = 250

    var called = false
    called = tracedTimer.time {
      Thread.sleep(duration) // Delay for yammer timer
      time.currentTime += duration
      true
    }

    called should be(true)
    yammerTimer.min should be > (0.0)
    verifyZeroInteractions(mockRecorder)
  }

  test("TracedTimer.update should trace and update timer within a tracing context") {

    val mockTimer = mock[Timer]
    val tracedTimer = new TracedTimer(mockTimer, "myName", Some("mySource"))

    val message = Message("myName", Some("mySource"))
    var context: Option[TraceContext] = None
    val duration = 1000

    tracer.trace() {
      context = tracer.currentContext
      tracedTimer.update(duration, TimeUnit.MILLISECONDS)
    }

    verify(mockTimer).update(duration, TimeUnit.MILLISECONDS)
    verify(mockRecorder).record(Record(context.get, time.currentTime, message, Some(duration)))
  }

  test("TracedTimer.update should trace and update timer within a tracing context with extra") {

    val mockTimer = mock[Timer]
    val tracedTimer = new TracedTimer(mockTimer, "myName", Some("mySource"))

    val message = Message("myName [extra=value]", Some("mySource"))
    var context: Option[TraceContext] = None
    val duration = 1000

    tracer.trace() {
      context = tracer.currentContext
      tracedTimer.update(duration, TimeUnit.MILLISECONDS, Map("extra" -> "value"))
    }

    verify(mockTimer).update(duration, TimeUnit.MILLISECONDS)
    verify(mockRecorder).record(Record(context.get, time.currentTime, message, Some(duration)))
  }

  test("TracedTimer.update should update timer without error outside a tracing context") {

    val mockTimer = mock[Timer]
    val tracedTimer = new TracedTimer(mockTimer, "myName", Some("mySource"))

    val duration = 1000

    tracedTimer.update(duration, TimeUnit.MILLISECONDS)

    verify(mockTimer).update(duration, TimeUnit.MILLISECONDS)
  }

  test("TracedTimer.timerContext should trace and use timerContext from Metrics") {

    val yammerTimer = new Timer(Metrics.defaultRegistry().newTimer(classOf[TracedTimer], "timer"))
    yammerTimer.clear()
    yammerTimer.min should be(0.0)

    val tracedTimer = new TracedTimer(yammerTimer, "myName", Some("mySource"))

    val message = Message("myName", Some("mySource"))
    var context: Option[TraceContext] = None
    val duration = 250

    tracer.trace() {
      context = tracer.currentContext
      val timerContext = tracedTimer.timerContext()
      Thread.sleep(duration) // Delay for yammer timer
      time.currentTime += duration
      timerContext.stop()
    }

    val endTime = yammerTimer.min

    yammerTimer.min should be > (0.0)
    verify(mockRecorder).record(Record(context.get, time.currentTime, message, Some(duration)))
    yammerTimer.min should be(endTime)
  }

  test("TracedTimer.timerContext should trace and use timerContext from Metrics with extra") {

    val yammerTimer = new Timer(Metrics.defaultRegistry().newTimer(classOf[TracedTimer], "timer"))
    yammerTimer.clear()
    yammerTimer.min should be(0.0)

    val tracedTimer = new TracedTimer(yammerTimer, "myName", Some("mySource"))

    val message = Message("myName [extra=value]", Some("mySource"))
    var context: Option[TraceContext] = None
    val duration = 250

    tracer.trace() {
      context = tracer.currentContext
      val timerContext = tracedTimer.timerContext()
      Thread.sleep(duration) // Delay for yammer timer
      time.currentTime += duration
      timerContext.stop(Map("extra" -> "value"))
    }

    val endTime = yammerTimer.min

    yammerTimer.min should be > (0.0)
    verify(mockRecorder).record(Record(context.get, time.currentTime, message, Some(duration)))
    yammerTimer.min should be(endTime)
  }

  test("TracedTimer.timerContext should update timer without error outside a tracing context") {

    val yammerTimer = new Timer(Metrics.defaultRegistry().newTimer(classOf[TracedTimer], "timer"))
    yammerTimer.clear()
    yammerTimer.min should be(0.0)

    val tracedTimer = new TracedTimer(yammerTimer, "myName", Some("mySource"))

    val duration = 250

    val timerContext = tracedTimer.timerContext()
    Thread.sleep(duration) // Delay for yammer timer
    time.currentTime += duration
    timerContext.stop()

    val endTime = yammerTimer.min

    yammerTimer.min should be > (0.0)
    verifyZeroInteractions(mockRecorder)
    yammerTimer.min should be(endTime)
  }
}
