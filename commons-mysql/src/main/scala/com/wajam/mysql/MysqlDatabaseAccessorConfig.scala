package com.wajam.mysql

case class MysqlDatabaseAccessorConfig(dbname: String,
                                       username: String,
                                       password: String,
                                       serverNames: Seq[String],
                                       database: String,
                                       initMasterPoolSize: Int,
                                       maxMasterPoolSize: Int,
                                       initSlavePoolSize: Int,
                                       maxSlavePoolSize: Int,
                                       checkoutTimeoutMs: Int,
                                       maxIdleTimeSec: Int,
                                       slaveMonitoringEnabled: Boolean,
                                       slaveMonitoringIntervalSec: Long,
                                       numHelperThread: Int,
                                       maxQueryTimeInSec: Int)
