package com.stratio.streaming.functions.ddl;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.AttributeAlreadyExistException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.stratio.streaming.common.StratioStreamingConstants;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.messages.BaseStreamingMessage;
import com.stratio.streaming.messages.ColumnNameTypeValue;
import com.stratio.streaming.utils.SiddhiUtils;

public class AlterStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(AlterStreamFunction.class);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public AlterStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	

	@Override
	public Void call(JavaRDD<BaseStreamingMessage> rdd) throws Exception {
		
		List<BaseStreamingMessage> requests = rdd.collect();
		
		for (BaseStreamingMessage request : requests) {
//			Siddhi doesn't throw an exception if stream does not exist and you try to remove it
//			so there is no need to check if stream exists before dropping it.
			if (getSiddhiManager().getInputHandler(request.getStreamName()) != null) {
				
				try {
					int addedColumns = enlargeStream(request);
					
					logger.info("==> ALTER: stream " + request.getStreamName() 
										+ " was enlarged with " + addedColumns
										+ " more new columns OK");
					
					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.OK);
					
				} catch (SiddhiPraserException se) {
					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_PARSER_ERROR);
				} catch (AttributeAlreadyExistException aoee) {
					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_COLUMN_ALREADY_EXISTS);
				} catch (Exception e) {
					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_GENERAL_ERROR);
				}
			}
			else {
				logger.info("==> ALTER: stream " + request.getStreamName() + " does not exist KO");
				ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
			
		}
		
		return null;
	}
	
	
	private int enlargeStream(BaseStreamingMessage request) {
		
		int addedColumns = 0;
		StreamDefinition streamMetaData = getSiddhiManager().getStreamDefinition(request.getStreamName());
		
		for (ColumnNameTypeValue columnNameTypeValue: request.getColumns()) {
			
//			Siddhi will throw an exception if you try to add a column that already exists, 
//			so we first try to find it in the stream
			if (!columnAlreadyExistsInStream(columnNameTypeValue.getColumn(), streamMetaData)) {
				
				addedColumns++;
				streamMetaData.attribute(columnNameTypeValue.getColumn(), SiddhiUtils.decodeSiddhiType(columnNameTypeValue.getType()));
				
				logger.info("==> ALTER: stream " + request.getStreamName() + " has now a new column: " + columnNameTypeValue.getColumn() + "|" + columnNameTypeValue.getType() + " OK");
			}
			else {
				logger.info("==> ALTER: stream " + request.getStreamName() + " already has this column: " + columnNameTypeValue.getColumn() + "|" + columnNameTypeValue.getType() + " KO");
				throw new AttributeAlreadyExistException(columnNameTypeValue.getColumn());
			}
		}
		
		return addedColumns;

	}
	
	private boolean columnAlreadyExistsInStream(String columnName, StreamDefinition streamMetaData) {
		
		for(Attribute column:  streamMetaData.getAttributeList()) {
			if (column.getName().equalsIgnoreCase(columnName)) {
				return true;
			}
		}
		
		return false;
	}
	

	
	
}
