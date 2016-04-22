@api
Feature: Stream stop listen
	stopListenStream method should stop an active to-topic dump and delete every active query 

	Background: 
		Given I drop every existing stream
		
	Scenario: Stop listening to a non-listened existing stream
		When I create a stream with name 'stopListen' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		And I stop listening to a stream with name 'stopListen'
		Then an exception 'IS NOT' thrown

	Scenario: Stop listening to a previously listened existing stream
		When I create a stream with name 'stopListen' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		When I listen to a stream with name 'stopListen'
		And I insert into a stream with name 'stopListen' this data:
			| 1 | a |
			| 2 | 4 |
		And I wait '5' seconds

		Then the stream 'stopListen' has 'LISTEN' as active actions
		And  the stream 'stopListen' has this content (with column name, type and value):
			| 1 String a | 2 Integer 4 |
			| 1 String b | 2 Integer 5 |
		When I stop listening to a stream with name 'stopListen'
		Then the stream 'stopListen' has '' as active actions

	@ignore @tillfixed(DECISION-299)
	Scenario: Stop listen to an non-existing stream			
		When I stop listening to a stream with name 'inexistentStream'		
		Then an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Stream .*? does not exists'			
			 					
	Scenario Outline: Stop listening to an bad named stream			
		When I stop listening to a stream with name '<streamName>'		
		Then an exception 'IS' thrown with class 'StratioAPISecurityException' and message like '<message>'			
			 
		Examples:
			 | streamName 	| message                     |
			 |          	| Stream name cannot be empty |
			 | //NULL// 	| Stream name cannot be null  |
