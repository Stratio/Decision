package com.stratio.bus

trait IStratioBus {

  def create(tableName: String, queryString: String)

  def insert(queryString: String)

  def select

}
