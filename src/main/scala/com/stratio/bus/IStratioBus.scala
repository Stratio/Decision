package com.stratio.bus

trait IStratioBus {

  def create(tableName: String, tableValues: Map[String, BusDataTypes.DataType])

  def insert

  def select

}
