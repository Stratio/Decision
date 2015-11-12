@api
Feature: Stream alter
	alterStream method should add the specified columns to an existing stream

	Scenario: Alter non-exixtent stream
		Given I drop every existing stream		
		When I alter a stream with name 'testStream', setting its columns (with type) as:
			| 1  | Boolean  |
		Then an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Stream .*? does not exists'
		
	Scenario: Alter pre-existing columns
		Given I drop every existing stream
		When I create a stream with name 'testStream' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		Then the count of created streams is '1'
		And the stream 'testStream' has this columns (with name and type):
			| 1 | String  |
			| 2 | Integer | 			
		And an exception 'IS NOT' thrown
			
		When I alter a stream with name 'testStream', setting its columns (with type) as:
			| 1  | Boolean  |
			| b  | String   |
		Then an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Column 1 already exists'
		
		When I alter a stream with name 'testStream', setting its columns (with type) as:
			| a  | Boolean  |
			| b  | String   |
		Then an exception 'IS NOT' thrown
		And the stream 'testStream' has this columns (with name and type):
			| 1  | String   |
			| 2  | Integer  |
			| a  | Boolean  |
			| b  | String   |		
			
		When I alter a stream with name 'testStream', setting its columns (with type) as:
			| 6  | Integer  |
			| 6  | String   |
		Then an exception 'IS' thrown  with class 'StratioEngineOperationException' and message like 'Column 6 already exists'
		And the stream 'testStream' has this columns (with name and type):
			| 1  | String   |
			| 2  | Integer  |
			| a  | Boolean  |
			| b  | String   |
			| 6  | Integer  |
			
	Scenario: Alter with non-existant column type
		When I alter a stream with name 'testStream', setting its columns (with type) as:
			| 33  | NiftyType  |
		Then an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Bad column definition at alter'
	
	Scenario: alter with empty columns
		When I alter a stream with name 'testStream', setting its columns (with type) as:
			||
		Then an exception 'IS' thrown with class 'StratioAPISecurityException' and message like 'Invalid column list'		
	
	Scenario: Alter a non-existing stream with a non-existant column type		
		When I alter a stream with name 'NonExistantTestStream', setting its columns (with type) as:
			| 33  | NiftyType  |
		Then an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Stream .*? does not exists'