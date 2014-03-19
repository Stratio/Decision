package com.stratio.streaming;

import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

import scala.Tuple2;

public class DataToCollector {

	public DataToCollector() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public static void sendDataToOpenSense(List<Tuple2<String, Object>> data) throws Exception {
		
		
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

      	
//      	*********************    THINGSPEAK API   *********************
//      	HttpPost httppost = new HttpPost("http://api.thingspeak.com/update");
//			
//		// Request parameters and other properties.
//  		List<NameValuePair> params = new ArrayList<NameValuePair>(2);
//  		params.add(new BasicNameValuePair("key", "WO4E0IP3HAQ43YIH"));
//  		
//			
//			for (int i=0; i<measures.size(); i++) {
//	  		    logger.info("==============================> measure:" + measures.get(i)._1() + " -- value: " + measures.get(i)._2());  				    		
//	    		params.add(new BasicNameValuePair("field" + measures.get(i)._1().charAt(measures.get(i)._1().length() - 1), ""+  measures.get(i)._2()));      				    		
//				
//			}
//			
//			httppost.setEntity(new UrlEncodedFormEntity(params));	
//      	*********************    THINGSPEAK API   *********************
			
			
	

//			Execute and get the response.
	    	HttpResponse response = httpclient.execute(httppost);
	}

}
