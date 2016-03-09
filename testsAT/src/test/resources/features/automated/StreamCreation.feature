@api
Feature: Stream creation
	createStream method should increment the stream count

	Scenario Outline: Initial stream creation
		Given I drop every existing stream
		When I create a stream with name '<streamName>' and columns (with type):
			| 향 | String  |
			| 2  | Integer |
		Then the count of created streams is '1'
		And the stream '<streamName>' has this columns (with name and type):
			| 향 | String  |
			| 2  | Integer |
		Examples:
			 | streamName 		 |
			 | testStream 		 |
			 | anotherTestStream |

	Scenario: Fixture deletion/creation
		Given I drop every existing stream
		When I create a stream with name 'anotherTestStream' and columns (with type):
			| X  | String  |
			| 2  | Integer |

	Scenario Outline: Consecutive stream creations
		When I create a stream with name '<streamName>' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		Then the count of created streams is '<streamCount>'
		And an exception '<exception>' thrown

		Examples:
			 | streamName        | streamCount | exception |
			 | testStream        | 2           | IS NOT    |
			 | anotherTestStream | 2           | IS        |
			 | AnotherTestStream | 3           | IS NOT    |

	Scenario Outline: Invalid names stream creations
	Given I drop every existing stream
		When I create a stream with name '<streamName>' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		And an exception 'IS' thrown with class 'StratioAPISecurityException' and message like 'Stream name cannot be <type>'
		Then the count of created streams is '<streamCount>'

		Examples:
			 | streamName | streamCount | type  |
			 |            | 0           | empty |
			 | //NULL//   | 0           | null  |
	         | "            | 0         |double quotes  |
			 | `            | 0         |  backtick  |
			 | '            | 0         |  quotes  |
			 | asd`         | 0         |  quotes  |
	         | 0x0008       | 0         |hexadecimal   |

	Scenario Outline: Reserved names stream creations
		Given I drop every existing stream
		When I create a stream with name '<streamName>' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		And an exception 'IS' thrown with class 'StratioAPISecurityException' and message like 'Operation create not allowed .*'
		Then the count of created streams is '<streamCount>'

		Examples:
			 | streamName                        | streamCount |
			 | stratio_stats_base                | 0           |
			 | stratio_stats_global_by_operation | 0           |

	Scenario: Nulled columns stream creations
		When I create a stream with name 'noColumnStream' and columns (with type):
			||
		Then an exception 'IS' thrown with class 'StratioAPISecurityException' and message like 'Invalid column list'
		And the count of created streams is '0'