package com.stratio.streaming.functions.dml;

import java.util.List;
import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import scala.Tuple2;

import com.google.common.collect.Lists;
import com.stratio.deep.entity.Cells;
import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.streams.StreamSharedStatus;
import com.stratio.streaming.utils.SiddhiUtils;

public class InsertIntoStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(InsertIntoStreamFunction.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public InsertIntoStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
		
		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {
			
//			stream exists
			if (getSiddhiManager().getStreamDefinition(request.getStreamName()) != null) {
				
				try {															
					
					getSiddhiManager()
						.getInputHandler(request.getStreamName())
							.send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(request.getStreamName()), 
																											request.getColumns()));
					
					
//					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.OK);
					
				} catch (AttributeNotExistException anee) {
					ackStreamingOperation(request, REPLY_CODES.KO_COLUMN_DOES_NOT_EXIST);
				}
				catch (InterruptedException e) {
					ackStreamingOperation(request, REPLY_CODES.KO_GENERAL_ERROR);
				}
				
				
			}
			else {
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
		}
		
		
//		JavaRDD<StratioStreamingMessage> save2cassandraRequests = rdd.filter(new Function<StratioStreamingMessage, Boolean>() {
//			
//			private List<String> enabledPersistenceStreamsList = Lists.newArrayList();
//			private List<String> disabledPersistenceStreamsList = Lists.newArrayList();
//
//			@Override
//			public Boolean call(StratioStreamingMessage request) throws Exception {
//				
//				if (enabledPersistenceStreamsList.contains(request.getStreamName())) {
//					return Boolean.TRUE;
//				}
//				if (disabledPersistenceStreamsList.contains(request.getStreamName())) {
//					return Boolean.FALSE;
//				}
//				
//				if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()).isSaveToCassandra_enabled()) {
//					enabledPersistenceStreamsList.add(request.getStreamName());
//					return Boolean.TRUE;
//				}
//				else {
//					disabledPersistenceStreamsList.add(request.getStreamName());
//					return Boolean.FALSE;
//				}				
//			}
//		});
//		
//		
//		
//		JavaRDD<Cells> outRDD = save2cassandraRequests.map(new Function<StratioStreamingMessage>, Cells>() {
//
//			@Override
//			public Cells call(StratioStreamingMessage request) throws Exception {
//				
//				Cells cells = new Cells();
//				
//				com.stratio.deep.entity.Cell<UUID> requestTimeUUIDCell = com.stratio.deep.entity.Cell.create("time_taken", UUIDGen.getTimeUUID(), true, false);
//				
//
//				for (Attribute column : getSiddhiManager().getStreamDefinition(request.getRequest_id()).getAttributeList()) {
//					
////					logger.info("event data size: " +ie.getData().length + " // attribute: " + column.getName() + " // position: " + streamDefinition.getAttributePosition(column.getName()));
//		
////					avoid retrieving a value out of the scope
////					outputStream could have more fields defined than the output events (projection)
//					if (event._2().length >= getSiddhiManager().getStreamDefinition(request.getRequest_id()).getAttributePosition(column.getName()) + 1) {
//						
//						switch (streamDefinition.getAttributeType(column.getName())) {
//							case STRING:
//								cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (String) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//								break;
//							case BOOL:
//								cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (Boolean) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//								break;
//							case DOUBLE:
//								cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (Double) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//								break;
//							case INT:
//								cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (Integer) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//								break;
//							case LONG:
//								cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (Long) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//								break;
//							case FLOAT:
//								cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (Float) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//								break;
//							default:
//								throw new SiddhiPraserException("Unsupported Column type");
//					}
//					
//						
//					}
//					
//					
//				}
//				
//				return cells;
//			}
//			
//		});
		
		
//		if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()).()) {
//			JavaRDD<Cells> outRDD = rdd.map(new Function<Tuple2<Long, Object[]>, Cells>() {
//			
//				@Override
//				public Cells call(Tuple2<Long, Object[]> event) throws Exception {
//					
//					Cells cells = new Cells();
//					
//					com.stratio.deep.entity.Cell<UUID>   requestTimeUUIDCell 		= com.stratio.deep.entity.Cell.create("time_taken", UUIDGen.getTimeUUID(), true, false);
//					
//
//					for (Attribute column : streamDefinition.getAttributeList()) {
//						
////													logger.info("event data size: " +ie.getData().length + " // attribute: " + column.getName() + " // position: " + streamDefinition.getAttributePosition(column.getName()));
//						
////													avoid retrieving a value out of the scope
////													outputStream could have more fields defined than the output events (projection)
//						if (event._2().length >= streamDefinition.getAttributePosition(column.getName()) + 1) {
//							
//							switch (streamDefinition.getAttributeType(column.getName())) {
//								case STRING:
//									cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (String) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//									break;
//								case BOOL:
//									cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (Boolean) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//									break;
//								case DOUBLE:
//									cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (Double) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//									break;
//								case INT:
//									cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (Integer) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//									break;
//								case LONG:
//									cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (Long) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//									break;
//								case FLOAT:
//									cells.add(com.stratio.deep.entity.Cell.create(column.getName(), (Float) event._2()[streamDefinition.getAttributePosition(column.getName())]));
//									break;
//								default:
//									throw new SiddhiPraserException("Unsupported Column type");
//						}
//						
//							
//						}
//						
//						
//					}
//					
//					return cells;
//					
//					
//					}
//				});
//	
//			
//				logger.debug("3333333333333333333333333333333333333333333333333333 " + streamDefinition.getStreamId() + "//" + rdd.count());	
//				CassandraCellRDD.cql3SaveRDDToCassandra(outRDD.rdd(), streamCassandraConfig);			
//		}
		
		return null;
	}

	

	



}
