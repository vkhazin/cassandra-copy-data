CREATE KEYSPACE perf8rst
WITH replication = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 1
}
AND durable_writes = true;