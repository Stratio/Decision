@api @MongoDB
Feature: Stop save to MongoDB
	stopSaveToMongo should leave the stream with no active action defined

	Background:
		Given I connect to 'Mongo' cluster at '${MONGO_HOST}'
		Given I drop every existing stream
		
	Scenario Outline: Stop saving
		When I create a stream with name '<streamName>' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		When I start saving to MongoDB a stream with name '<streamName>'
		And the stream '<streamName>' has 'SAVE_TO_MONGO' as active actions
		When I stop saving to MongoDB a stream with name '<streamName>'
		Then an exception 'IS NOT' thrown
		And the stream '<streamName>' has '' as active actions

		Examples:
		| streamName    |
		| testMnumber   |
		| 2testM_number |
		| testMnumber   |
		| testMstring   |
		| testmstring   |

	Scenario Outline: Stop saving bad streams
		When I stop saving to MongoDB a stream with name '<streamName>'
		Then an exception 'IS' thrown with class 'StratioAPISecurityException' and message like '<message>'

		Examples:
		| streamName | message                     |
		|            | Stream name cannot be empty |
		| //NULL//   | Stream name cannot be null  |