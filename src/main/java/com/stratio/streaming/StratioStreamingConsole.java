package com.stratio.streaming;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jline.ConsoleReader;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
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

import EDU.oswego.cs.dl.util.concurrent.Executor;
import ca.zmatrix.cli.ParseCmd;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.stratio.streaming.common.StratioStreamingConstants;
import com.stratio.streaming.messages.BaseStreamingMessage;
import com.stratio.streaming.messages.ColumnNameTypeValue;
import com.stratio.streaming.messages.ListStreamsMessage;

public class StratioStreamingConsole {
	
	
	private static Logger logger = LoggerFactory.getLogger(StratioStreamingConsole.class);
	private String brokerList;
	private String sessionId;
	private Producer<String, String> producer;
	private CuratorFramework  client;
	private ExecutorService consumers;
	



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
            
            if (line.startsWith(StratioStreamingConstants.STREAM_OPERATIONS.ACTION.LISTEN.toLowerCase()) 
            		|| line.startsWith(StratioStreamingConstants.STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA.toLowerCase()) 
            		|| line.startsWith(StratioStreamingConstants.STREAM_OPERATIONS.ACTION.SAVETO_DATACOLLECTOR.toLowerCase()) 
            		|| line.startsWith(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.ADD_QUERY.toLowerCase()) 
            		|| line.startsWith(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.ALTER.toLowerCase()) 
            		|| line.startsWith(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.CREATE.toLowerCase()) 
            		|| line.startsWith(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.DROP.toLowerCase())
            		|| line.startsWith(StratioStreamingConstants.STREAM_OPERATIONS.MANIPULATION.INSERT.toLowerCase())
            		|| line.startsWith(StratioStreamingConstants.STREAM_OPERATIONS.MANIPULATION.LIST.toLowerCase())) {
            	
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
        
        consumers = Executors.newFixedThreadPool(1);
        consumers.execute(new StratioRepliesConsumer(R.get("--zookeeper").toString(), StratioStreamingConstants.BUS.LIST_STREAMS_TOPIC));
        
        
        
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
					System.out.println("<<<<<<<<<< REPLY FROM STRATIO STREAMING FOR REQUEST ID: " + decodeReplyFromStratioStreaming(Integer.valueOf(new String(client.getData().forPath(event.getPath())))));
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
		consumers.shutdownNow();
		producer.close();
		client.close();
		System.out.println("<<<<<<<<<< SHUTTING DOWN STRATIO BUS CONNECTION");
	}
    
    
    

	
	
	private void handleCommand(String request) {
		
		
		try {
			BaseStreamingMessage message = MessageFactory.getMessageFromCommand(request, sessionId);
			
			
			
			
			System.out.println("==> Sending message to Stratio Streaming: " + new Gson().toJson(message));
			KeyedMessage<String, String> busMessage = new KeyedMessage<String, String>(StratioStreamingConstants.BUS.TOPICS, request.split("@")[0].trim(), new Gson().toJson(message));
			producer.send(busMessage);
			
			
			client.checkExists().watched().forPath(StratioStreamingConstants.STREAMING.ZK_BASE_PATH + "/" + message.getOperation() + "/" + message.getRequest_id());						
			
			
		} catch (Exception e) {
			System.out.println("Oooooooops, can't handle your command, maybe this can help: " + e.getMessage());
		}
	}
	
	
	private String decodeReplyFromStratioStreaming(Integer code) {
		
		String decodedReply = "";
		
		switch (code) {
		case 1:
			decodedReply = "OK";
			break;
		case 2:
			decodedReply = "KO: PARSER ERROR";
			break;
		case 3:
			decodedReply = "KO: STREAM ALREADY EXISTS";
			break;
		case 4:
			decodedReply = "KO: STREAM DOES NOT EXIST";
			break;
		case 5:
			decodedReply = "KO: QUERY ALREADY EXISTS";
			break;
		case 6:
			decodedReply = "KO: LISTENER ALREADY EXISTS";
			break;		
		case 7:
			decodedReply = "KO: GENERAL ERROR";
			break;				
		case 8:
			decodedReply = "KO: COLUMN ALREADY EXISTS";
			break;							
		case 9:
			decodedReply = "KO: COLUMN DOES NOT EXIST";
			break;	
		default:
			break;
		}
		
		
		return decodedReply;
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
	
	
	private class StratioRepliesConsumer implements Runnable {
		
		private String zkCluster;
		private ConsumerConnector consumer;
		private String topic;
		
		public StratioRepliesConsumer(String zkCluster, String topic) {
			this.zkCluster = zkCluster;			
			this.topic = topic;
			//		start consumer
			this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zkCluster));		
		}		
		
		
		private ConsumerConfig createConsumerConfig(String zkClusters) {
	        Properties props = new Properties();
	        props.put("zookeeper.connect", zkClusters);
	        props.put("group.id", "314");
	 
	        return new ConsumerConfig(props);
	    }
		
		private String printColumns(List<ColumnNameTypeValue> columns) {
			String print = "(";
			
			for (ColumnNameTypeValue column : columns) {
				print += column.getColumn() + "." + column.getType() + ",";
			}
			
			if (print.length() > 1) {
				print = print.substring(0, print.length() -1);
			}
						
			return print += ")";
			
		}
		

		@Override
		public void run() {
			
			try {
			
				while (!Thread.currentThread().isInterrupted()) {
	
					
				
					Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
					topicCountMap.put(topic, new Integer(1));
					Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
					ConsumerIterator<byte[], byte[]> it = consumerMap.get(topic).get(0).iterator();
					
						        
					while (it.hasNext()) {
						ListStreamsMessage reply = new Gson().fromJson(new String(it.next().message()), ListStreamsMessage.class);
						
						System.out.println("");
						System.out.println("*********** LIST STREAMS REPLY ************");
						System.out.println("**** SIDDHI STREAMS: " + reply.getCount());
						for (BaseStreamingMessage stream : reply.getStreams()) {
							System.out.println("********" + stream.getStreamName() + printColumns(stream.getColumns()));
						}
						System.out.println("*******************************************");
						System.out.println("");
						
					}
				}
			}
			catch(Exception e) {
				
			}
			finally {
				System.out.println("<<<<<<<<<<<< Shutting down consumer for topic:" + topic);
				consumer.shutdown();
			}
			
		
				
		
			
		}
		
	}
	
	
	
	private static class MessageFactory {
		
		private MessageFactory() {
			
		}
		
		
		private static BaseStreamingMessage getMessageFromCommand(String command, String sessionId) {
			
			String operation = command.split("@")[0].trim().replaceAll("\\s+","");
			
			
			if (!(operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.ALTER)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.CREATE)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.MANIPULATION.INSERT)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.ADD_QUERY)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.DROP)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.ACTION.LISTEN)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.MANIPULATION.LIST))) {
				
				throw new IllegalArgumentException("Unsupported command: " + command);
			}
		
			
			if ((operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.ALTER)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.CREATE)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.MANIPULATION.INSERT)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.ADD_QUERY))
				&& 	command.split("@").length != 3) {
				
				throw new IllegalArgumentException("Malformed request, missing or exceding parts: " + command);
			}
			
			if ((operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.DROP)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.ACTION.SAVETO_DATACOLLECTOR)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.ACTION.LISTEN))
				&& command.split("@").length != 2) {
				
				throw new IllegalArgumentException("Malformed request, missing or exceding parts: " + command);
			}
			
			if (operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.MANIPULATION.LIST)
					&& command.split("@").length != 1) {
				throw new IllegalArgumentException("Malformed request, missing or exceding parts: " + command);
			}
			
			
			BaseStreamingMessage message = new BaseStreamingMessage();
			String request = "";
			String stream = "";
			
			
			
			if (command.split("@").length != 1) {
				stream  = command.split("@")[1].trim().replaceAll("\\s+","");
			}
			
			if (command.split("@").length == 3) {
				request = StringUtils.removeEnd(StringUtils.removeStart(command.split("@")[2].trim(), "("), ")");
			}
			
			
			if (operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.ALTER)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.CREATE)
					|| operation.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.MANIPULATION.INSERT)) {
					
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
				
				if (command.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.CREATE) 
						|| command.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.ALTER)) {
					
					decodedColumns.add(new ColumnNameTypeValue(firstPart, secondPart, null));
				}
				if (command.equalsIgnoreCase(StratioStreamingConstants.STREAM_OPERATIONS.MANIPULATION.INSERT)) {
					decodedColumns.add(new ColumnNameTypeValue(firstPart, null, secondPart));
				}
				
				
			}
			
			return decodedColumns;
		}	
		
		
		
	}
	
	


	
	
	
}
	