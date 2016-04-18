@api @elasticsearch
Feature: Index to elasticsearch
	index should backup the whole stream to elasticsearch

	Background:
		Given I connect to 'Elasticsearch' cluster at '${ES_NODE}'
		Given I drop every existing stream
		Given I drop every existing elasticsearch index

	Scenario Outline: Indexing a neat stream
		When I create a stream with name '<streamName>' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		When I index a stream with name '<streamName>'
		Then the stream '<streamName>' has 'SAVE_TO_ELASTICSEARCH' as active actions
		When I insert into a stream with name '<streamName>' this data:
			| 1 | a |
			| 2 | 4 |
		When I insert into a stream with name '<streamName>' this data:
			| 1 | b |
			| 2 | 5 |
		When I wait '30' seconds
		Then an exception 'IS NOT' thrown
		And I execute an elasticsearch query over index 'stratiodecision' and mapping '<convertedStreamName>' and column '1' with value 'equals' to 'a'
		And I execute an elasticsearch query over index 'stratiodecision' and mapping '<convertedStreamName>' and column '2' with value 'equals' to '4'
		Then There are results found with:
			| 1 | 2 |
		    | a | 4 |
		And I execute an elasticsearch query over index 'stratiodecision' and mapping '<convertedStreamName>' and column '1' with value 'equals' to 'b'
		And I execute an elasticsearch query over index 'stratiodecision' and mapping '<convertedStreamName>' and column '2' with value 'equals' to 'c'
		Then There are results found with:
			| 1 | 2 |
			| b | 5 |

		Examples:
		| streamName   | convertedStreamName |
		| "            | "                 |
		| testESnumber | testESnumber        |
		| 0x0008       | \u0008                 |
		| '            | '                 |
		| /            | /               |
		|  korean:향    |  korean:향          |
		| 0x0000       | \u0000			 |


	Scenario Outline: Index a stream, delete it, recreate it with different contract and listen again
		When I create a stream with name '<streamName>' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		When I index a stream with name '<streamName>'
		Then the stream '<streamName>' has 'SAVE_TO_ELASTICSEARCH' as active actions
		When I insert into a stream with name '<streamName>' this data:
			| 1 | a |
			| 2 | 4 |
		When I insert into a stream with name '<streamName>' this data:
			| 1 | b |
			| 2 | 5 |
		When I wait '20' seconds
		Then an exception 'IS NOT' thrown
		And I execute an elasticsearch query over index 'stratiodecision' and mapping '<streamName>' and column '1' with value 'equals' to 'b'
		And I execute an elasticsearch query over index 'stratiodecision' and mapping '<streamName>' and column '2' with value 'equals' to '5'
		Then There are results found with:
			| 1 | 2 |
			| b | 5 |
		And I execute an elasticsearch query over index 'stratiodecision' and mapping '<streamName>' and column '1' with value 'equals' to 'a'
   		And I execute an elasticsearch query over index 'stratiodecision' and mapping '<streamName>' and column '2' with value 'equals' to '4'
   		Then There are results found with:
   			| 1 | 2 |
   		    | a | 4 |

		Given I drop every existing stream
		Given I drop an elasticsearch index named 'stratiodecision'
		When I create a stream with name '<streamName>' and columns (with type):
			| c1  | String  |
		Then the stream '<streamName>' has '' as active actions
		When I index a stream with name '<streamName>'
		Then the stream '<streamName>' has 'SAVE_TO_ELASTICSEARCH' as active actions
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
