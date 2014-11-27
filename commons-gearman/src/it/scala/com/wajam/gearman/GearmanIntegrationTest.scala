package com.wajam.gearman

import org.gearman._
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }

import com.wajam.gearman.utils.GearmanJson

abstract class GearmanIntegrationTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  protected val testFunctionName = "wajam_gearman_it_test"

  //Create the service
  private val gearmanService = new Gearman()
  //Create the server
  private val gearmanServer = gearmanService.createGearmanServer
  //Create a worker
  private val gearmanWorker = gearmanService.createGearmanWorker
  //Create a dummy job to register to worker
  private val dummyJob: GearmanFunction = new GearmanFunction {
    def work(job: GearmanJob): GearmanJobResult = {
      GearmanJson.decodeFromJson(job.getJobData) match {
        case data if data.isInstanceOf[Map[_, Any]] =>
          val dataMap = data.asInstanceOf[Map[String, Any]]
          dataMap.get("success") match {
            case Some(v) => GearmanJobResult.workSuccessful(job.getJobData)
            case None => GearmanJobResult.workFailed()
          }
        case _ => GearmanJobResult.workFailed()
      }
    }
  }

  override protected def beforeAll() {
    //Open the server port
    this.gearmanServer.openPort()
    //Register the function to the worker
    this.gearmanWorker.addFunction(testFunctionName, dummyJob)
    //Register the worker to the server
    this.gearmanWorker.addServer(gearmanServer)
  }

  override protected def afterAll() {
    this.gearmanWorker.unregisterAll()
    this.gearmanServer.closeAllPorts()
    this.gearmanService.shutdown()
  }

}
