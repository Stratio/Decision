/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package com.stratio.streaming.utils;
//
//import java.util.List;
//
//import org.apache.http.HttpResponse;
//import org.apache.http.client.HttpClient;
//import org.apache.http.client.methods.HttpPost;
//import org.apache.http.entity.StringEntity;
//import org.apache.http.impl.client.HttpClients;
//
//import scala.Tuple2;
//
//import com.google.common.collect.Lists;
//import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
//import com.stratio.streaming.commons.messages.StratioStreamingMessage;
//
//public class DataToCollectorUtils {
//
//	public DataToCollectorUtils() {
//		// TODO Auto-generated constructor stub
//	}
//
//	
//	
//	
//	public static void sendData(List<StratioStreamingMessage> collected_events) {
//		
//		ColumnNameTypeValue indexColumn = new ColumnNameTypeValue("index", null, null);
//		ColumnNameTypeValue dataColumn = new ColumnNameTypeValue("data", null, null);
//		
//		List<Tuple2<String, Object>> data = Lists.newArrayList();
//		
//		try {
//		
//			for (StratioStreamingMessage event : collected_events) {
//				
//				int indexPosition = event.getColumns().indexOf(indexColumn);
//				int dataPosition  = event.getColumns().indexOf(dataColumn);
//				
//				if (indexPosition > 0 & dataPosition > 0) {
////					TODO improve index formatting
//					data.add(new Tuple2<String, Object>(event.getColumns().get(indexPosition).getValue().toString().replace(".0", ""), 
//																event.getColumns().get(dataPosition).getValue()));
//				}
//				
//				
//				
//				
//			}
//			
//		
//			DataToCollectorUtils.sendDataToOpenSense(data);
//			
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		
//	}
//	
//	public static void sendDataToOpenSense(List<Tuple2<String, Object>> data) throws Exception {
//		
//		
//      	HttpClient httpclient = HttpClients.createDefault();
//      	
//      	
////  	*********************    OPEN SENSE API   ********************* 			        	
//      	HttpPost httppost = new HttpPost("http://api.sen.se/events/?sense_key=vHQBxu09Bgc1fl32zsiAfg");
//      	
//      	    		
//		String postBody = "";
//		
//		
//		
//		for (int i=0; i<data.size(); i++) {			
//			if (data.get(i)._2() instanceof String)  {
//				postBody = postBody + "{\"feed_id\":" + data.get(i)._1() + ",\"value\":\"" + data.get(i)._2() + "\"}";
//			}
//			if (data.get(i)._2() instanceof Double)  {
//				postBody = postBody + "{\"feed_id\":" + data.get(i)._1() + ",\"value\":" + (Math.round(((Double)data.get(i)._2()) * 100.0 ) / 100.0) + "}";
//			}
//			if (data.get(i)._2() instanceof Long)  {
//				postBody = postBody + "{\"feed_id\":" + data.get(i)._1() + ",\"value\":" + (Math.round(((Long)data.get(i)._2()) * 100.0 ) / 100.0) + "}";
//			}
//		
//		}
//			
//			
//			
//			httppost.setEntity(new StringEntity("[" + postBody.replace("}{", "},{") + "]"));	  						
////  	*********************    OPEN SENSE API   ********************* 
//
//      	
////      	*********************    THINGSPEAK API   *********************
////      	HttpPost httppost = new HttpPost("http://api.thingspeak.com/update");
////			
////		// Request parameters and other properties.
////  		List<NameValuePair> params = new ArrayList<NameValuePair>(2);
////  		params.add(new BasicNameValuePair("key", "WO4E0IP3HAQ43YIH"));
////  		
////			
////			for (int i=0; i<measures.size(); i++) {
////	  		    logger.info("==============================> measure:" + measures.get(i)._1() + " -- value: " + measures.get(i)._2());  				    		
////	    		params.add(new BasicNameValuePair("field" + measures.get(i)._1().charAt(measures.get(i)._1().length() - 1), ""+  measures.get(i)._2()));      				    		
////				
////			}
////			
////			httppost.setEntity(new UrlEncodedFormEntity(params));	
////      	*********************    THINGSPEAK API   *********************
//			
//			
//	
//
////			Execute and get the response.
//	    	HttpResponse response = httpclient.execute(httppost);
//	}
//
//}
