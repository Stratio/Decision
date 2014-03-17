package com.stratio.bus

trait IStratioBus {

  def create(queryString: String)

  def insert(queryString: String)

  def select

}
