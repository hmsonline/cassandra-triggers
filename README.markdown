
Cassandra Triggers is a lightweight mechanism to implement trigger-like functionality for Cassandra.

It uses AOP within the Cassandra Server itself to record mutation events in a column family.  
A thread then polls that column family for new events and executes the appropriate triggers that are subscribe to those events.
Upon successful processing of those events by all subscribed triggers the event is removed from the column family.