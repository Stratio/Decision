package com.stratio.bus

trait IStratioBus {
  def initialize(): IStratioBus

  def withZookeeperPort(port: String): IStratioBus

  def withKafkaPort(port: String): IStratioBus

  def create(queryString: String)

  def insert(queryString: String)

  def select(queryString: String)

  def alter(queryString: String)

  def drop(queryString: String)


}
