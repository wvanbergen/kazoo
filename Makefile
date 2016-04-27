.PHONY: confluent/kafka/* confluent/zookeeper/* confluent/registry/* confluent/start confluent/stop


# Confluent platform tasks

confluent/start: confluent/rest/start

confluent/stop: confluent/rest/stop confluent/registry/stop confluent/kafka/stop confluent/zookeeper/stop

# Download & extract tasks

confluent/confluent.tgz:
	mkdir -p confluent && wget http://packages.confluent.io/archive/2.0/confluent-2.0.1-2.11.7.tar.gz -O confluent/confluent.tgz

confluent/EXTRACTED: confluent/confluent.tgz
	tar xzf confluent/confluent.tgz -C confluent --strip-components 1 && mkdir confluent/logs && touch confluent/EXTRACTED
	echo "delete.topic.enable=true" >> confluent/etc/kafka/server.properties

# Zookeeper tasks

confluent/zookeeper/start: confluent/EXTRACTED
	nohup confluent/bin/zookeeper-server-start confluent/etc/kafka/zookeeper.properties 2> confluent/logs/zookeeper.err > confluent/logs/zookeeper.out < /dev/null &
	while ! nc localhost 2181 </dev/null; do echo "Waiting for zookeeper..."; sleep 1; done

confluent/zookeeper/stop: confluent/EXTRACTED
	confluent/bin/zookeeper-server-stop

# Kafka tasks

confluent/kafka/start: confluent/zookeeper/start confluent/EXTRACTED
	nohup confluent/bin/kafka-server-start confluent/etc/kafka/server.properties 2> confluent/logs/kafka.err > confluent/logs/kafka.out < /dev/null &
	while ! nc localhost 9092 </dev/null; do echo "Waiting for Kafka..."; sleep 1; done

confluent/kafka/stop: confluent/EXTRACTED
	confluent/bin/kafka-server-stop

# schema-registry tasks

confluent/registry/start: confluent/kafka/start confluent/EXTRACTED
	nohup confluent/bin/schema-registry-start confluent/etc/schema-registry/schema-registry.properties 2> confluent/logs/schema-registry.err > confluent/logs/schema-registry.out < /dev/null &
	while ! nc localhost 8081 </dev/null; do echo "Waiting for schema registry..."; sleep 1; done

confluent/registry/stop: confluent/EXTRACTED
	confluent/bin/kafka-server-stop

# REST proxy tasks

confluent/rest/start: confluent/registry/start confluent/EXTRACTED
	nohup confluent/bin/kafka-rest-start confluent/etc/kafka-rest/kafka-rest.properties 2> confluent/logs/kafka-rest.err > confluent/logs/kafka-rest.out < /dev/null &
	while ! nc localhost 8082 </dev/null; do echo "Waiting for REST proxy..."; sleep 1; done

confluent/rest/stop: confluent/EXTRACTED
	confluent/bin/kafka-rest-stop
