package com.wajam.mysql

import org.scalatest.FunSuite
import java.sql.SQLException

class TestMySqlDatabaseAccessor extends FunSuite {

  val TIMEOUT_MAGIC_CONSTANT = 12

  test("should fail quickly if cannot connect to database") {
    val timeout = 100
    val db = new MysqlDatabaseAccessor(new MysqlDatabaseAccessorConfig("name",
      username = "test",
      password = "test",
      serverNames = Seq("fakeserver"),
      database = "test",
      initMasterPoolSize = 1,
      maxMasterPoolSize = 1,
      initSlavePoolSize = 1,
      maxSlavePoolSize = 1,
      checkoutTimeoutMs = timeout,
      maxIdleTimeSec = 1,
      slaveMonitoringEnabled = false,
      slaveMonitoringIntervalSec = 1,
      numHelperThread = 1,
      maxQueryTimeInSec = 0))


    val s = System.currentTimeMillis
    intercept[SQLException] {
      db.executeSelect("Select 1=1", (_) => fail("Callback called"))
    }
    val timeTaken = System.currentTimeMillis - s
    assert(timeTaken < TIMEOUT_MAGIC_CONSTANT * timeout, timeTaken)
  }

}
