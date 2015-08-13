# Kazoo

Ruby library to access and manipulate Kafka metadata in Zookeeper

## Usage

First, make kazoo available by adding it to your Gemfile

``` ruby
gem 'kazoo-ruby', require: 'kazoo'
```

Now, you can interact with the cluster metadata as follows:

``` ruby
# Connect to the Zookeeper cluster that backs your Kafka cluster
cluster = Kazoo::Cluster.new('zookeeper1:2181,zookeeper2:2181/chroot')

# List the brokers that form the Kafka cluster
cluster.brokers.each do |id, broker|
  puts "Broker #{broker.id}: #{broker.addr}"
end

# Inspect topic/partition metadata
cluster.topics.each do |name, topic|
  puts "#{name}: #{topic.partitions.length} partitions"
end

# List consumers
cluster.consumergroups.each do |name, group|
  puts "Consumer #{name}: #{group.instances.length} running instances"
end
```

## Binary

This gem also comes with a simple `kazoo` binary to inspect your kafka cluster:

```
# Describe the brokers that compose the cluster
$ kazoo cluster --zookeeper zk1:2181,zk2:2181,zk3:2181/chroot

# List all topics or partitions in the cluster
$ kazoo topics --zookeeper zk1:2181,zk2:2181,zk3:2181/chroot
$ kazoo partitions --topic=access_log --zookeeper zk1:2181,zk2:2181,zk3:2181/chroot

```

## Contributing

1. Fork it ( https://github.com/wvanbergen/kazoo/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## See also

- [kafka-consumer](https://github.com/wvanbergen/kafka-consumer): a high-level
  Kafka consumer library that coordinates running instances using Zookeeper.
