package com.stratio.streaming;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import jline.ConsoleReader;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.zmatrix.cli.ParseCmd;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.stratio.streaming.commons.constants.BUS;
import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.ListStreamsMessage;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.commons.messages.StreamQuery;

public class StratioStreamingConsole {
	
	
	private static Logger logger = LoggerFactory.getLogger(StratioStreamingConsole.class);
	private String brokerList;
	private String sessionId;
	private Producer<String, String> producer;
	private CuratorFramework  client;
	



    public static void main(String[] args) throws Exception {

    	StratioStreamingConsole self = new StratioStreamingConsole();
    	
    	ConsoleReader reader = new ConsoleReader();
        reader.setDefaultPrompt("stratio_streaming>");
        reader.setBellEnabled(false);
        
        self.start(args);
       

        String line;
        PrintWriter out = new PrintWriter(System.out);

        while ((line = reader.readLine("stratio_streaming>")) != null) {
            
            if(line.trim().equals("")) {
        		continue;
        	}                        
            if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
            	self.shutDown();
                break;
            }
            
            line = line.toLowerCase();
            
            if (line.startsWith(STREAM_OPERATIONS.ACTION.LISTEN.toLowerCase())
            		|| line.startsWith(STREAM_OPERATIONS.ACTION.STOP_LISTEN.toLowerCase())
            		|| line.startsWith(STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA.toLowerCase()) 
            		|| line.startsWith(STREAM_OPERATIONS.ACTION.SAVETO_DATACOLLECTOR.toLowerCase()) 
            		|| line.startsWith(STREAM_OPERATIONS.DEFINITION.ADD_QUERY.toLowerCase())
            		|| line.startsWith(STREAM_OPERATIONS.DEFINITION.REMOVE_QUERY.toLowerCase()) 
            		|| line.startsWith(STREAM_OPERATIONS.DEFINITION.ALTER.toLowerCase()) 
            		|| line.startsWith(STREAM_OPERATIONS.DEFINITION.CREATE.toLowerCase()) 
            		|| line.startsWith(STREAM_OPERATIONS.DEFINITION.DROP.toLowerCase())
            		|| line.startsWith(STREAM_OPERATIONS.MANIPULATION.INSERT.toLowerCase())
            		|| line.startsWith(STREAM_OPERATIONS.MANIPULATION.LIST.toLowerCase())) {
            	
            	self.handleCommand(line);
            	continue;
            }
            
            
            
            
            out.println("======> Hey, i don't what to do with -> " + line);
            out.flush();
            
        }
    }
    
    
	private void start(String[] args) throws Exception {
		
//		DECODING ARGUMENTS FROM COMMAND LINE
		String usage = "usage: --broker-list ip:port";
        ParseCmd cmd = new ParseCmd.Builder()
        							.help(usage)                          
        							.parm("--broker-list", "node.stratio.com:9999" ).rex("^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]):[0-9]{1,4}+(,(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]):[0-9]{1,4}+)*$").req()
        							.parm("--zookeeper",   "node.stratio.com:9999" ).rex("^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]):[0-9]{1,4}+(,(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]):[0-9]{1,4}+)*$").req()
        							.build();  
       
        HashMap<String, String> R = new HashMap<String,String>();
        String parseError    = cmd.validate(args);
        if( cmd.isValid(args) ) {
            R = (HashMap<String, String>) cmd.parse(args);
            logger.info("Launching with these params:"); 
            logger.info(cmd.displayMap(R));
        }
        else { 
        	logger.error(parseError); 
        	System.exit(1); 
        } 
        
        
        this.brokerList = R.get("--broker-list").toString();
        this.sessionId = "" + System.currentTimeMillis();
        this.producer = new Producer<String, String>(createProducerConfig());
              
        
        
//		ZOOKEPER CONNECTION
		client = CuratorFrameworkFactory.newClient(R.get("--zookeeper"), 25*1000, 10*1000, new ExponentialBackoffRetry(1000, 3));
		

		client.start();
		client.getZookeeperClient().blockUntilConnectedOrTimedOut();
		
		if (!client.isStarted()) {
			 throw new Exception("Connection to Zookeeper timed out after seconds");
		}
		
		client.getCuratorListenable().addListener(new CuratorListener() {

			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
//				client.getChildren().watched().forPath("/test/test");
				
				
				switch (event.getType()) {
				case WATCHED:
					String originalReply = "";
					try {
						originalReply = new String(client.getData().forPath(event.getPath()));
						Integer reply = Integer.valueOf(originalReply);
						System.out.println("<<<<<<<<<< REPLY FROM STRATIO STREAMING FOR REQUEST ID: " + REPLY_CODES.getReadableErrorFromCode(reply));
					}
					catch(NumberFormatException nfe) {
						printListStreams(originalReply);
					}
					
					break;
					
				case CLOSING:
					System.out.println("<<<<<<<<<< SHUTTING DOWN ZK LISTENER");
					break;

				default:
					System.out.println(event.getType() + " Unknown reply from stratio streaming");
				}
				
			}
			
		});
		
        
		System.out.println(">>>>>> Connected to Stratio Bus");
        System.out.println(">>>>>> Your Session ID in Stratio Streaming is " + this.sessionId);
	}
	
	private void shutDown() {
		producer.close();
		client.close();
		System.out.println("<<<<<<<<<< SHUTTING DOWN STRATIO BUS CONNECTION");
	}
    
    
	private void printListStreams(String originalReply) {
		ListStreamsMessage reply = new Gson().fromJson(originalReply, ListStreamsMessage.class);
		
		System.out.println("");
		System.out.println(originalReply);
		System.out.println("");
		System.out.println("*********** LIST STREAMS REPLY ************");
		System.out.println("**** SIDDHI STREAMS: " + reply.getCount());
		for (StratioStreamingMessage stream : reply.getStreams()) {
			System.out.println("********" + stream.getStreamName() + printColumnsAndQueries(stream.getColumns(), stream.getQueries()));
		}
		System.out.println("*******************************************");
		System.out.println("");
	}
    
	private String printColumnsAndQueries(List<ColumnNameTypeValue> columns, List<StreamQuery> queries) {
		String printColumns = "";
		for (ColumnNameTypeValue column : columns) {
			printColumns += column.getColumn() + "." + column.getType() + ",";
		}
		
		if (printColumns.length() > 0) {
			printColumns = printColumns.substring(0, printColumns.length() -1);
		}
		
		String printQueries = "";
		for (StreamQuery query : queries) {
			printQueries += query.getQueryId() + "." + query.getQuery() + ",";
		}
		
		if (printQueries.length() > 0) {
			printQueries = printQueries.substring(0, printQueries.length() -1);
		}
					
		return ", columns: [" + printColumns + "], queries: [" + printQueries + "]";
		
	}		
	
	
	private void handleCommand(String request) {
		
		
		try {
			StratioStreamingMessage message = MessageFactory.getMessageFromCommand(request, sessionId);
			
			
			
			
			System.out.println("==> Sending message to Stratio Streaming: " + new Gson().toJson(message));
			KeyedMessage<String, String> busMessage = new KeyedMessage<String, String>(BUS.TOPICS, request.split("@")[0].trim(), new Gson().toJson(message));
			producer.send(busMessage);
			
			
			client.checkExists().watched().forPath(STREAMING.ZK_BASE_PATH + "/" + message.getOperation() + "/" + message.getRequest_id());						
			
			
		} catch (Exception e) {
			System.out.println("Oooooooops, can't handle your command, maybe this can help: " + e.getMessage());
		}
	}
	
	
	private ProducerConfig createProducerConfig() {
		Properties properties = new Properties();
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("metadata.broker.list", brokerList);
//		properties.put("request.required.acks", "1");
//		properties.put("compress", "true");
//		properties.put("compression.codec", "gzip");
//		properties.put("producer.type", "sync");
 
        return new ProducerConfig(properties);
    }	
	
	
	private static class MessageFactory {
		
		private MessageFactory() {
			
		}
		
		
		private static StratioStreamingMessage getMessageFromCommand(String command, String sessionId) {
			
			String operation = command.split("@")[0].trim().replaceAll("\\s+","");
			
			
			if (!(operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.ALTER)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.CREATE)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.MANIPULATION.INSERT)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.ADD_QUERY)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.REMOVE_QUERY)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.DROP)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.ACTION.LISTEN)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.ACTION.STOP_LISTEN)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.MANIPULATION.LIST))) {
				
				throw new IllegalArgumentException("Unsupported command: " + command);
			}
		
			
			if ((operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.ALTER)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.CREATE)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.MANIPULATION.INSERT)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.ADD_QUERY)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.REMOVE_QUERY))
				&& 	command.split("@").length != 3) {
				
				throw new IllegalArgumentException("Malformed request, missing or exceding parts: " + command);
			}
			
			if ((operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.DROP)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.ACTION.SAVETO_DATACOLLECTOR)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.ACTION.LISTEN)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.ACTION.STOP_LISTEN))
				&& command.split("@").length != 2) {
				
				throw new IllegalArgumentException("Malformed request, missing or exceding parts: " + command);
			}
			
			if (operation.equalsIgnoreCase(STREAM_OPERATIONS.MANIPULATION.LIST)
					&& command.split("@").length != 1) {
				throw new IllegalArgumentException("Malformed request, missing or exceding parts: " + command);
			}
			
			
			StratioStreamingMessage message = new StratioStreamingMessage();
			String request = "";
			String stream = "";
			
			
			
			if (command.split("@").length != 1) {
				stream  = command.split("@")[1].trim().replaceAll("\\s+","");
			}
			
			if (command.split("@").length == 3) {
				request = StringUtils.removeEnd(StringUtils.removeStart(command.split("@")[2].trim(), "("), ")");
			}
			
			
			if (operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.ALTER)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.CREATE)
					|| operation.equalsIgnoreCase(STREAM_OPERATIONS.MANIPULATION.INSERT)) {
					
				message.setColumns(decodeColumns(operation, request));
			}

			message.setRequest(request.trim());
			message.setOperation(operation);
			message.setStreamName(stream);
			message.setRequest_id("" + System.currentTimeMillis());
			message.setSession_id(sessionId);
			message.setTimestamp(System.currentTimeMillis());
			
			return message;
			
		}
		
		
		
		
		
		private static List<ColumnNameTypeValue> decodeColumns(String command, String request) {
			
			List<ColumnNameTypeValue> decodedColumns = Lists.newArrayList();
			String[] columns = request.split(",");
			
			if (columns.length == 0) {
				throw new IllegalArgumentException("No columns found");
			}
			
			
			for (String column : columns) {
				
				if (column.split("\\.").length <= 1) {
					throw new IllegalArgumentException("Error parsing columns");
				}
				
				String firstPart = column.split("\\.")[0].trim().replaceAll("\\s+","");
				String secondPart = column.split("\\.")[1].trim().replaceAll("\\s+","");
				
				if (command.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.CREATE) 
						|| command.equalsIgnoreCase(STREAM_OPERATIONS.DEFINITION.ALTER)) {
					
					decodedColumns.add(new ColumnNameTypeValue(firstPart, secondPart, null));
				}
				if (command.equalsIgnoreCase(STREAM_OPERATIONS.MANIPULATION.INSERT)) {
					decodedColumns.add(new ColumnNameTypeValue(firstPart, null, secondPart));
				}
				
				
			}
			
			return decodedColumns;
		}	
		
		
		
	}
	
	


	
	
	
}
	