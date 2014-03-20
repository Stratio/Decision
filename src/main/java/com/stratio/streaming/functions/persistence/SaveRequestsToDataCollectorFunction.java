package com.stratio.streaming.functions.persistence;

import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import scala.Tuple2;

import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.messages.BaseStreamingMessage;

public class SaveRequestsToDataCollectorFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(SaveRequestsToDataCollectorFunction.class);
	private String cassandraCluster;
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public SaveRequestsToDataCollectorFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
		this.cassandraCluster = cassandraCluster; 
	}
	

	@Override
	public Void call(JavaRDD<BaseStreamingMessage> rdd) throws Exception {
		
				
//			TODO when siddhi RDD is ready

		return null;
	}
	
	
	
	private static void sendDataToOpenSense(List<Tuple2<String, Object>> data) throws Exception {
		
		
      	HttpClient httpclient = HttpClients.createDefault();
      	
      	
//  	*********************    OPEN SENSE API   ********************* 			        	
      	HttpPost httppost = new HttpPost("http://api.sen.se/events/?sense_key=vHQBxu09Bgc1fl32zsiAfg");
      	
      	    		
		String postBody = "";
		
		
		
		for (int i=0; i<data.size(); i++) {			
			if (data.get(i)._2() instanceof String)  {
				postBody = postBody + "{\"feed_id\":" + data.get(i)._1() + ",\"value\":\"" + data.get(i)._2() + "\"}";
			}
			else {
				postBody = postBody + "{\"feed_id\":" + data.get(i)._1() + ",\"value\":" + (Math.round(((Double)data.get(i)._2()) * 100.0 ) / 100.0) + "}";
			}
		
		}
			
			
			
		httppost.setEntity(new StringEntity("[" + postBody.replace("}{", "},{") + "]"));	  						
//  	*********************    OPEN SENSE API   ********************* 
	

//		Execute and get the response.
	    HttpResponse response = httpclient.execute(httppost);
	}

}
