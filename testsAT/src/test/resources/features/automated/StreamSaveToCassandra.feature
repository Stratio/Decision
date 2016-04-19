@api @C*
Feature: Save to cassandra
	saveToCassandra should dump a stream to a cassandra backend

	Background:
		Given I drop every existing stream
		Given I connect to 'Cassandra' cluster at '${CASSANDRA_HOST}'
		When I create a stream with name 'testC*number' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		When I create a stream with name '2testCnumber' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		When I create a stream with name 'testCnumber' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		When I create a stream with name 'testCstring' and columns (with type):
			| col1  | String  |
			| col2  | Integer |			
		When I create a stream with name 'testcstring' and columns (with type):
			| col3  | String  |
			| col4  | Integer |
			
	Scenario: Saving from an unexistent stream
		When I start saving to Cassandra a stream with name 'unexistantStream'
		Then an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Stream .* does not exist'

	Scenario Outline: Saving from an bad named stream
		When I start saving to Cassandra a stream with name '<streamName>'
		Then an exception 'IS' thrown with class 'StratioAPISecurityException' and message like '<message>'

		Examples:
		| streamName | message                     |
		|            | Stream name cannot be empty |
		| //NULL//   | Stream name cannot be null  |
		| 0x0008     | Bad stream name             |

	Scenario Outline: Saving from an existent stream with an unnacepted equivalent C* table name
		When I start saving to Cassandra a stream with name '<streamName>'
		Then an exception 'IS ' thrown with class 'com.stratio.decision.commons.exceptions.StratioAPISecurityException' and message like 'Stream name <streamName> is not compatible with SAVE_TO_CASSANDRA action.'
		And the stream '<streamName>' has '' as active actions

		Examples:
		| streamName   |
		| testC*number |
		| 2testCnumber |

	Scenario: Saving from an existent stream
		When I create a Cassandra keyspace named 'stratio_decision'
		When I start saving to Cassandra a stream with name 'testcstring'
		When I start saving to Cassandra a stream with name 'testCstring'
		And I wait '10' seconds
		Then an exception 'IS NOT' thrown
		And the stream 'testCstring' has 'SAVE_TO_CASSANDRA' as active actions
		When I wait '10' seconds
		And I insert into a stream with name 'testCstring' this data:
			| col1 | 'a' |
			| col2 | 4 |
		And I insert into a stream with name 'testcstring' this data:
			| col3 | 'b' |
			| col4 | 5 |
		And I wait '10' seconds
		Then a Cassandra keyspace 'stratio_decision' exists
		And a Casandra keyspace 'stratio_decision' contains a table 'testcstring'
		And a Casandra keyspace 'stratio_decision' contains a table 'testCstring'
		And a Casandra keyspace 'stratio_streaming' contains a table 'testcstring' with '1' rows
		And a Casandra keyspace 'stratio_streaming' contains a table 'testCstring' with '1' rows
		And a Casandra keyspace 'stratio_streaming' contains a table 'testcstring' with values:
			| col1-text | col2-int |
			| b         | 5        |
		And a Casandra keyspace 'stratio_streaming' contains a table 'testCstring' with values:
			| col1-text | col2-int |
			| a         | 4        |


	Scenario: Saving from an existent stream with existent table
		When I start saving to Cassandra a stream with name 'testCstring'
		When I start saving to Cassandra a stream with name 'testcstring'
		And I insert into a stream with name 'testCstring' this data:
			| col1 | a |
			| col2 | 4 |
		And I insert into a stream with name 'testcstring' this data:
			| col3 | b |
			| col4 | 5 |
		Then an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Cassandra table already exists'

	Scenario Outline: Stop saving
		When I start saving to Cassandra a stream with name '<streamName>'
		And the stream '<streamName>' has 'SAVE_TO_CASSANDRA' as active actions
		When I stop saving to Cassandra a stream with name '<streamName>'
		Then an exception 'IS NOT' thrown
		And the stream '<streamName>' has '' as active actions

		Examples:
		| streamName   |
		| testCstring  |
		| testcstring  |
		| testCnumber  |

	Scenario Outline: Stop saving bad streams
		When I stop saving to Cassandra a stream with name '<streamName>'
		Then an exception 'IS' thrown with class 'StratioAPISecurityException' and message like '<message>'

		Examples:
		| streamName | message                     |
		|            | Stream name cannot be empty |
		| //NULL//   | Stream name cannot be null  |