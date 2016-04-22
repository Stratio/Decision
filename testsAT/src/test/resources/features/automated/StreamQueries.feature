@api
Feature: Query addition
	addQuery should generate new streams if the initial stream columns match any of the query ones

	Background:
		Given I drop every existing stream
		When I create a stream with name 'testAddQuery' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
		When I create a stream with name 'testAddQuery2' and columns (with type):
			| 1  | String  |
			| 2  | Integer |
			
	Scenario Outline: Adding a nice formatted query
		When I add a query '<query>' to a stream with name 'testAddQuery'
		And I insert into a stream with name 'testAddQuery' this data:
			| 1 | a |
			| 2 | 4 |
		And I insert into a stream with name 'testAddQuery' this data:
			| 1 | b |
			| 2 | 5 |
		And I wait '10' seconds
		Then the stream 'testAddQuery' has this query: '<query>'
		And the count of created streams is '3'
		And the stream 'testAddQuery' has '1' query
		And the stream 'testAddQueryOutput' has this columns (with name and type):
			| col2 | Integer |
		And the stream 'testAddQueryOutput' has '0' query
		When I listen to a stream with name 'testAddQueryOutput'
		And I wait '60' seconds
		Then the stream 'testAddQueryOutput' has this content (with column name, type and value):
			| col2 Integer 4.0 |
			| col2 Integer 5.0 |

		Examples:
		| query |
		| from testAddQuery select 2 as col2 insert into testAddQueryOutput |

	Scenario Outline: Create query and drop generated
		When I add a query '<query>' to a stream with name 'testAddQuery2'
		And I insert into a stream with name 'testAddQuery2' this data:
			| 1 | a |
			| 2 | 4 |
		And I insert into a stream with name 'testAddQuery2' this data:
			| 1 | b |
			| 2 | 5 |
		And I wait '10' seconds
		And I delete the stream 'testAddQuery2Output'
		Then an exception 'IS' thrown with class 'StratioAPISecurityException' and message like 'Cannot delete the internal stream'

		Examples:
		| query |
		| from testAddQuery2 select '2' as col2 insert into testAddQuery2Output |

	@ignore @tillfixed(DECISION-299)
	Scenario Outline: Create badly formed query so its id will be an empty string
		When I add a query '<query>' to a stream with name 'testAddQuery2'
		Then an exception 'IS' thrown with class '<exception>' and message like '<message>'

		Examples:
		| query                                                                   | exception                       | message                    |
		| from testAddQuery2 UNselect '2' as col2 insert into testAddQuery2Output | StratioEngineOperationException | SiddhiParserException      |
		|                                                                         | StratioEngineOperationException | SiddhiParserException      |
		| //NULL//                                                                | StratioAPISecurityException     | Stream name cannot be null |

	Scenario Outline: Drop main stream. Related queries and streams must be wiped
		When I add a query '<query>' to a stream with name 'testAddQuery2'
		And I insert into a stream with name 'testAddQuery2' this data:
			| 1 | a |
			| 2 | 4 |
		And I insert into a stream with name 'testAddQuery2' this data:
			| 1 | b |
			| 2 | 5 |
		And I wait '10' seconds
		When I delete the stream 'testAddQuery2'
		Then an exception 'IS NOT' thrown
		And the count of created streams is '1'

		Examples:
		| query |
		| from testAddQuery2 select '2' as col2 insert into testAddQuery2Output |

	Scenario Outline: Drop a query. Related streams must be wiped
		When I add a query '<query>' to a stream with name 'testAddQuery2'
		And I insert into a stream with name 'testAddQuery2' this data:
			| 1 | a |
			| 2 | 4 |
		And I insert into a stream with name 'testAddQuery2' this data:
			| 1 | b |
			| 2 | 5 |
		And I wait '10' seconds
		And I remove a query '<query>' to a stream with name 'testAddQuery2'
		Then an exception 'IS NOT' thrown
		And the count of created streams is '2'

		Examples:
		| query                                                                 |
		| from testAddQuery2 select '2' as col2 insert into testAddQuery2Output |

	@ignore @tillfixed(DECISION-299)
	Scenario Outline: Drop query with nulled values involved.
		When I remove a query '<query>' to a stream with name '<streamName>'
		Then an exception 'IS' thrown with class '<exception>' and message like '<message>'

		Examples:
		| streamName     | query                                                                 | exception                   | message                    |
		| //NULL//      | from testAddQuery2 select '2' as col2 insert into testAddQuery2Output | StratioAPISecurityException | Stream name cannot be null |
		| testAddQuery  | //NULL//                                                              | StratioAPISecurityException | Query cannot be null       |

	@ignore @tillfixed(DECISION-299)
	Scenario Outline: Drop inexistant query
		When I add a query 'from testAddQuery select "2" as col2 insert into testAddQueryOutput' to a stream with name 'testAddQuery'
		When I remove a query 'foo' to a stream with name 'testAddQuery'
		And I wait '10' seconds
		Then the stream 'testAddQuery' has this query: '<query>'
		And the count of created streams is '3'
		And an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Query -333 in stream .*? does not exists'

		Examples:
		| query |
		| from testAddQuery select "2" as col2 insert into testAddQueryOutput |

	@ignore @tillfixed(DECISION-299)
	Scenario Outline: Inexistant stream query drop
		When I add a query 'from testAddQuery select "2" as col2 insert into testAddQueryOutput' to a stream with name 'testAddQuery'
		When I remove a query '<query>' to a stream with name 'bar'
		And I wait '10' seconds
		And an exception 'IS' thrown with class 'StratioEngineOperationException' and message like 'Stream .*? does not exists'

		Examples:
		| query |
		| from testAddQuery select "2 as col2 insert into testAddQueryOutput |

	Scenario Outline: Nice query deletion
		When I add a query 'from testAddQuery select "2" as col2 insert into testAddQueryOutput' to a stream with name 'testAddQuery'
		When I remove a query '<query>' to a stream with name 'testAddQuery'
		And I wait '10' seconds
		And an exception 'IS NOT' thrown
		And the count of created streams is '2'

		Examples:
		| query |
		| from testAddQuery select "2" as col2 insert into testAddQueryOutput |