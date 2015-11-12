@shell
Feature: Shell help and version commands should answer with some nice messages
	
	@ignore
	Scenario: help command for streaming shell must output formatted hints		
		When I type 'help' to the streaming shell		
		Then the shell must output the string '* ! - Allows execution of operating system (OS) commands'
		Then the shell must output the string '* // - Inline comment markers (start of line only)'
		Then the shell must output the string '* ; - Inline comment markers (start of line only)'
		Then the shell must output the string '* add query - create new query'
		Then the shell must output the string '* alter - alter existing stream'
		Then the shell must output the string '* clear - Clears the console'
		Then the shell must output the string '* cls - Clears the console'
		Then the shell must output the string '* columns - lists every column in a existing stream'
		Then the shell must output the string '* create - create new stream'
		Then the shell must output the string '* date - Displays the local date and time'
		Then the shell must output the string '* drop - drop existing stream'
		Then the shell must output the string '* exit - Exits the shell'
		Then the shell must output the string '* help - List all commands usage'
		Then the shell must output the string '* index start - index stream events'
		Then the shell must output the string '* index stop - stop index stream events'
		Then the shell must output the string '* insert - insert events into existing stream'
		Then the shell must output the string '* list - list all streams into engine'
		Then the shell must output the string '* listen start - attach stream to kafka topic'
		Then the shell must output the string '* listen stop - de-attach stream to kafka topic'
		Then the shell must output the string '* quit - Exits the shell'
		Then the shell must output the string '* remove query - remove an existing query'
		Then the shell must output the string '* save cassandra start - start save to cassandra action'
		Then the shell must output the string '* save cassandra stop - stop save to cassandra action'
		Then the shell must output the string '* save mongo start - start save to mongo action'
		Then the shell must output the string '* save mongo stop - stop save to mongo action'
		Then the shell must output the string '* script - Parses the specified resource file and executes its commands'
		Then the shell must output the string '* system properties - Shows the shell\'s properties'
		Then the shell must output the string '* version - Displays shell version'

	Scenario: version command for streaming shell must output a dotted based version
		When I type 'version' to the streaming shell		
		Then the shell must output the string '0\.5\.0'