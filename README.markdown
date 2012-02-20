Cassandra Triggers is a lightweight mechanism to implement trigger-like functionality for Cassandra.

# Rationale

Cassandra is an excellent flexible, scalable storage mechanism, but for many reasons it may be necessary to keep other systems in synch with the data stored in Cassandra.  For example, you may want to keep a SOLR instance up to date to support real-time search queries, or you may need to keep an RDBMS up-to-date to support reporting or ad hoc queries (queries that you couldn't build into your cassandra data model because they can't be known ahead of time)  Even within Cassandra itself, you may want to create additional column families that use different data models to support querying. (wide rows, etc.)

All of this logic could be built into the application, but if you want to shield your clients from all that complexity, you could build that into triggers.

Maxim Grinev does a wonderful job capturing the motivation for triggers in his [blog post](http://maxgrinev.com/2010/07/23/extending-cassandra-with-asynchronous-triggers/).  

# The Implementation

Our implementation uses AOP within the Cassandra Server itself to record mutation events in a column families.  A thread then polls that column family for new events and executes the appropriate triggers subscribed to those events. Upon successful processing of those events by all subscribed triggers the event is removed from the column family.

To get started, look at:

[Installation](https://github.com/hmsonline/cassandra-triggers/wiki/installation)

[Configuration](https://github.com/hmsonline/cassandra-triggers/wiki/configuration)

