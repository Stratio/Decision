@api @elasticsearch
Feature: Stream stop index
	stopIndexStream method should stop a to-elasticsearch dump and remove the stream status action "INDEX"

	Background: 
		Given I drop every existing stream
		
	Scenario: Stop indexing a non-listened existing stream			
		When I create a stream with name 'stopIndex' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		And I stop indexing a stream with name 'stopIndex'
		Then an exception 'IS NOT' thrown

	Scenario: Stop indexing a previously indexed existing stream
		When I create a stream with name 'stopIndex' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		When I index a stream with name 'stopIndex'	
		And I insert into a stream with name 'stopIndex' this data:
			| 1 | a | 
			| 2 | 4 |
		And I wait '5' seconds
		Then the stream 'stopIndex' has 'SAVE_TO_ELASTICSEARCH' as active actions
		When I stop indexing a stream with name 'stopIndex'
		Then the stream 'stopIndex' has '' as active actions		

	@ignore @tillfixed(DECISION-299)
	Scenario: Stop indexing a non-existing stream			
		When I stop indexing a stream with name 'inexistentStream'		
		Then an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Stream .*? does not exists'			
			 					
	Scenario Outline: Stop indexing a bad named stream
		When I stop indexing a stream with name '<streamName>'
		Then an exception 'IS' thrown with class 'StratioAPISecurityException' and message like '<message>'			
			 
		Examples:
			 | streamName 	| message                     |
			 |          	| Stream name cannot be empty |
			 | //NULL// 	| Stream name cannot be null  |
