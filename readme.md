# program to fix indices stuck in ilm due to repository change

The program will fix indices that get stuck trying to move to frozen because the repository was changed by:
- mouting the index as a partial snapshot backed by the old repository 
- issueing ilm move to frozen, complete
- switching the existing index with the new index in the datastream
- delete the old index

Program is designed to exit when anything unexpected happens so the problem can be inspected before proceeding.
Some effort is taken to make all the actions idempotent: 
- mounting action can be retried but if it fails because the index is already there it will assume the action succeeded
- ilm move is repeated to get it to the correct state