@ignore @manual @api
Feature: Create a stream, operate over it and persist to Elasticsearch

  Scenario: Data passthrough and the result is persisted to Elasticsearch
    Given I start a new StreamingAPI session
    When I start saving to Elasticsearch a stream with name '${STREAM_NAME}'
    And I create a stream with name '${STREAM_NAME}' and columns (with type):
      | name  | String  |
      | data1  | Integer |
      | data2  | Float |
    And I insert into a stream with name '${STREAM_NAME}' this data:
      | name | a |
      | data1 | 1 |
      | data2 | 2.3 |

    Then a Mongo dataBase '.+' contains a table '.+' with values:
      | name | a |
      | data1 | 1 |
      | data2 | 2.3 |



  Scenario: Data passthrough with a downed input
    Given I start a new StreamingAPI session
    When input connection is down
    Then an exception '.+' thrown with class '(.+' and message like '(.+'))
    And the decision service status should be RUNNING.

  Scenario: Data passthrough to Elasticsearch but Elasticsearch is down
    Given I start a new StreamingAPI session
    When I start saving to Elasticsearch a stream with name '${STREAM_NAME}'
    And I create a stream with name '${STREAM_NAME}' and columns (with type):
      | name  | String  |
      | data1  | Integer |
      | data2  | Float |
    And I insert into a stream with name '${STREAM_NAME}' this data:
      | name | a |
      | data1 | 1 |
      | data2 | 2.3 |
    Then an exception '.+' thrown with class '(.+' and message like '(.+'))
    And the decision service status should be RUNNING.

  Scenario: Data passthrough to Elasticsearch not matching model given
    Given I start a new StreamingAPI session
    When I start saving to Elasticsearch a stream with name '${STREAM_NAME}'
    And I create a stream with name '${STREAM_NAME}' and columns (with type):
      | name  | String  |
      | data1  | Integer |
      | data2  | Float |
    And I insert into a stream with name '${STREAM_NAME}' this data:
      | name | a |
      | data2 | 2.3 |
    Then an exception '.+' thrown with class '(.+' and message like '(.+'))
    And the decision service status should be RUNNING.


  Scenario Outline: Data is filtered though a valid query and the result is saved to Elasticsearch
    Given I start a new StreamingAPI session
    When I start saving to Elasticsearch a stream with name '${STREAM_NAME}'
    And I create a stream with name '${STREAM_NAME}' and columns (with type):
      | name  | String  |
      | data1  | Integer |
      | data2  | Float |
    And I insert into a stream with name '${STREAM_NAME}' this data:
      | name | a |
      | data1 | 1 |
      | data2 | 2.3 |
      | name | b |
      | data1 | 2 |
      | data2 | 2.3 |
      | name | c |
      | data1 | 3 |
      | data2 | 2.3 |
      | name | d |
      | data1 | 4 |
      | data2 | 2.3 |
      | name | e |
      | data1 | 5 |
      | data2 | 2.3 |
      | name | e |
      | data1 | 7 |
      | data2 | 2.3 |
    And I add a query '<filtering_query>' to a stream with name '${STREAM_NAME}'
    Then a Mongo dataBase '.+' contains a table '.+' with values:
      | name | <val1> |
      | data1 | <val2> |
      | data2 | <val3> |

  Examples:
      | filtering_query | val1 | val2 | val3 |
      | from ${STREAM_NAME} [data1 > 6] select data1 insert into ${STREAM_NAME}-query | e | 7 | 2.3 |
      | from ${STREAM_NAME} [data1 % 5] select sum(data1) as data group by name insert into ${STREAM_NAME}-query | e | 5 | 2.3 |
      | from ${STREAM_NAME} [data1 > 6] select avg(data1) as data group by name insert into  ${STREAM_NAME}-query| e | 6 | 2.3 |

  Scenario Outline: Data is filtered though a wrong query
    Given I start a new StreamingAPI session
    When I start saving to Elasticsearch a stream with name '${STREAM_NAME}'
    And I create a stream with name '${STREAM_NAME}' and columns (with type):
      | name  | String  |
      | data1  | Integer |
      | data2  | Float |
    And I insert into a stream with name '${STREAM_NAME}' this data:
      | name | a |
      | data1 | 1 |
      | data2 | 2.3 |
      | name | b |
      | data1 | 2 |
      | data2 | 2.3 |
      | name | c |
      | data1 | 3 |
      | data2 | 2.3 |
      | name | d |
      | data1 | 4 |
      | data2 | 2.3 |
      | name | e |
      | data1 | 5 |
      | data2 | 2.3 |
      | name | e |
      | data1 | 7 |
      | data2 | 2.3 |
    And I add a query '<wrong_query>' to a stream with name '${STREAM_NAME}'
    Then an exception '<error>' thrown with class '(.+' and message like '(.+'))

    Examples:
      | wrong_query | error |
      | fprm ${STREAM_NAME} select name, avg(data1) as data group by name insert into  ${STREAM_NAME}-query| wrong syntax |
      | from ${STREAM_NAME} select name, avg(name) as data group by name insert into  ${STREAM_NAME}-query| wrong data   |
      | from ${STREAM_NAME} select non-existent, avg(data1) as data group by name insert into  ${STREAM_NAME}-query| wrong model  |
