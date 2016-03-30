@api
Feature: Setting a milliseconds value for timing out a communication.
	Any operation with a low ack time should timeout always. Ack can be setup just once for scenario and just before other operation.

	Background:
		When I set a '1' milliseconds ACK timeout

	Scenario: Wipe every stream on a previous scenario so it wont collide with setting the timeout
		When I set a '10000' milliseconds ACK timeout
		Given I drop every existing stream
		When I wait '10' seconds

	Scenario: Create Stream on low ack must timeout (even it will succeed eventually)
		When I create a stream with name 'testStreamACK0' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		Then an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Acknowledge timeout expired'

	Scenario: Timeout set after any operation wont be honored
		When I set a '10000' milliseconds ACK timeout
		And I create a stream with name 'testStreamACK1' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		Then an exception 'IS NOT' thrown
		And the stream 'testStreamACK1' has this columns (with name and type):
			| 1 | String  |
			| 2 | Integer |
		When I set a '1' milliseconds ACK timeout
		And I create a stream with name 'testStreamACK2' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		Then an exception 'IS NOT' thrown
		And the stream 'testStreamACK2' has this columns (with name and type):
			| 1 | String  |
			| 2 | Integer |

	Scenario: Alter Stream on low ack must timeout (even it will succeed eventually)
		When I alter a stream with name 'testStreamACK0', setting its columns (with type) as:
			| 3  | Boolean  |
		Then an exception 'IS' thrown with class 'StratioEngineConnectionException' and message like 'Acknowledge timeout expired'

	Scenario: Drop Stream on low ack must timeout (even it will succeed eventually)
		When I delete the stream 'testStreamACK0'
		Then an exception 'IS' thrown with class 'StratioEngineConnectionException' and message like 'Acknowledge timeout expired'

	Scenario Outline: Non valid ack timeout must be forbidden
		When I set a '<timeout>' milliseconds ACK timeout
		When I create a stream with name '<streamName>' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		Then an exception 'IS' thrown with class 'StratioEngineConnectionException' and message like '<message>'

		Examples:
		| streamName     | timeout  | message                              |
		| testStreamACK5 | -1       | timeout value must be a positive int |
		| testStreamACK6 | 0        | timeout value must be a positive int |
		| testStreamACK7 | //NULL// | Stream name cannot be null           |
