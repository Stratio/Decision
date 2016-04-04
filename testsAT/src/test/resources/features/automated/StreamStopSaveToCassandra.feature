@api @C*
Feature: Stop save to cassandra
	stopSaveToCassandra should leave the stream with no active action defined

	Background:
		Given I drop every existing stream
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