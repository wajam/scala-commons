package com.wajam.commons.hbase

sealed trait DataEntity[K <: Key]
    extends KeyImplicits {

  def key: K
}

trait UniqueDataEntity extends DataEntity[Key] {
  def key: Key
}

trait CompoundDataEntity extends DataEntity[CompoundKey] {
  def key: CompoundKey

  def scanFromKey: Key = key.min

  def scanToKey: Key = key.max
}

