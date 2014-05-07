package com.stratio.streaming.utils

import com.google.gson.Gson
import com.stratio.streaming.commons.messages.ListStreamsMessage
import com.stratio.streaming.commons.streams.StratioStream
import scala.collection.JavaConversions._

object StreamsParser {

  def parse(json: String) = {
    val listStreams = new Gson().fromJson(json, classOf[ListStreamsMessage]).getStreams.toList
    val stratioStreams = listStreams.map(stream => {
      new StratioStream(stream.getStreamName,
        stream.getColumns,
        stream.getQueries,
        stream.isUserDefined)}
    )
    stratioStreams
  }

}
