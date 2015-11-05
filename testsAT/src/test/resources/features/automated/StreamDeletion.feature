@api
Feature: Stream deletion operation
	deleteStream method should effectively delete the mentioned stream. 
	In case of a non-existant one, it shoult throw an exception

	Scenario: Previous cleaunup
		Given I drop every existing stream
		
	Scenario Outline: System stream deletion 		
		When I delete the stream '<streamToDelete>'
		Then an exception 'IS' thrown with class 'StratioAPISecurityException' and message like 'Operation drop not allowed in stream .*?'
		And the count of created streams is '<streamCount>' 		

		Examples:
			 | streamToDelete                    | streamCount |
     		 | stratio_stats_base                | 0           |
     		 | stratio_stats_global_by_operation | 0           |
     		 
	Scenario Outline: Existing stream deletion 		
		When I create a stream with name '<streamName>' and columns (with type):
			| 1       | String  |
			| 2       | Integer |
		And I wait '1' seconds
		And I delete the stream '<streamToDelete>'
		Then an exception 'IS NOT' thrown		
		And the count of created streams is '0' 		

		Examples:
			 | streamName	       | streamToDelete     | exception | 
			 | auditorias          | auditorias         | IS NOT    | 
			 | estadisticas        | estadisticas   	| IS NOT    | 
			 | '":             	   | '":           		| IS NOT    | 
			 | testDeletion 	   | testDeletion 	    | IS NOT  	| 

	Scenario Outline: Non-existing stream deletion 		
		When I delete the stream '<streamName>'
		Then an exception 'IS' thrown with class '<exceptionClass>' and message like '<message>'
		And the count of created streams is '0'
		
		Examples:
			 | streamName   |  exceptionClass                 | message             |
			 | testDeletion | StratioEngineOperationException | .*? does not exists |
			 | //NULL//     | StratioAPISecurityException     | Stream name cannot be null |
			 |              | StratioAPISecurityException     | invalid stream name |