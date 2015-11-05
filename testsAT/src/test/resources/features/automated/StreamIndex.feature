@api @elasticsearch
Feature: Index to elasticsearch
	index should backup the whole stream to elasticsearch

	Background:
		Given I drop every existing stream
		And I empty every existing elasticsearch index

	Scenario Outline: Indexing a neat stream
		When I create a stream with name '<streamName>' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		When I index a stream with name '<streamName>'
		Then the stream '<streamName>' has 'INDEXED' as active actions
		When I insert into a stream with name '<streamName>' this data:
			| 1 | a | 
			| 2 | 4 |
		When I insert into a stream with name '<streamName>' this data:
			| 1 | b | 
			| 2 | 5 |
		When I wait '30' seconds	
		Then an exception 'IS NOT' thrown
		And the 'stratiostreaming' index has a type '<convertedStreamName>' with content (key and value): '1:b,2:5' 
		And the 'stratiostreaming' index has a type '<convertedStreamName>' with content (key and value): '1:a,2:4'

		Examples:
		| streamName   | convertedStreamName | 
		| "            | %22                 |
		| testESnumber | testESnumber        |
		| 0x0008       | %08                 |
		| '            | %27                 |
		| /            | %2F                 |
		|  korean:향       |  korean:향                        |
		| 0x0000       | %00				 |
	
	Scenario Outline: Indexing an unnacepted stream name
		When I create a stream with name '<streamName>' and columns (with type):
			| c1  | String  |
		When I index a stream with name '<streamName>'
		Then the stream '<streamName>' has '' as active actions
		Then an exception 'IS' thrown  with class 'StratioAPISecurityException' and message like 'Stream name .*? is not compatible with INDEX action'
		
		Examples:
		| streamName   |
		|              |

	Scenario Outline: Index a stream, delete it, recreate it with different contract and listen again
		When I create a stream with name '<streamName>' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		When I index a stream with name '<streamName>'
		Then the stream '<streamName>' has 'INDEXED' as active actions
		When I insert into a stream with name '<streamName>' this data:
			| 1 | a | 
			| 2 | 4 |
		When I insert into a stream with name '<streamName>' this data:
			| 1 | b | 
			| 2 | 5 |
		When I wait '20' seconds	
		Then an exception 'IS NOT' thrown
		And the 'stratiostreaming' index has a type '<streamName>' with content (key and value): '1:b,2:5' 
		And the 'stratiostreaming' index has a type '<streamName>' with content (key and value): '1:a,2:4'
		
		Given I drop every existing stream
		When I create a stream with name '<streamName>' and columns (with type):
			| c1  | String  |			
		Then the stream '<streamName>' has '' as active actions
		When I index a stream with name '<streamName>'
		Then the stream '<streamName>' has 'INDEXED' as active actions
		When I insert into a stream with name '<streamName>' this data:
			| c1 | a | 
		When I insert into a stream with name '<streamName>' this data:
			| c1 | b | 
		When I wait '20' seconds	
		Then an exception 'IS NOT' thrown

		Examples:
		| streamName   |  
		| a            | 

	Scenario Outline: Index from a an non-existing stream			
		When I index a stream with name '<streamName>'		
		Then an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Stream <streamName> does not exists'			
			 
		Examples:
			 | streamName 	             | 
			 | inexistentIndexTestStream |
			 