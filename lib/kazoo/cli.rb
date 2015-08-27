require 'kazoo'
require 'thor'

module Kazoo
  class CLI < Thor
    class_option :zookeeper, type: :string, default: ENV['ZOOKEEPER']

    desc "cluster", "Describes the Kafka cluster as registered in Zookeeper"
    def cluster
      validate_class_options!

      kafka_cluster.brokers.values.sort_by(&:id).each do |broker|
        $stdout.puts "#{broker.id}:\t#{broker.addr}\t(hosts #{broker.replicated_partitions.length} partitions, leads #{broker.led_partitions.length})"
      end
    end

    desc "topics", "Lists all topics in the cluster"
    def topics
      validate_class_options!

      kafka_cluster.topics.values.sort_by(&:name).each do |topic|
        $stdout.puts topic.name
      end
    end

    desc "create_topic", "Creates a new topic"
    option :name, type: :string, required: true
    option :partitions, type: :numeric, required: true
    option :replication_factor, type: :numeric, required: true
    def create_topic
      validate_class_options!

      kafka_cluster.create_topic(options[:name], partitions: options[:partitions], replication_factor: options[:replication_factor])
    end

    desc "delete_topic", "Removes a topic"
    option :name, type: :string, required: true
    def delete_topic
      validate_class_options!

      kafka_cluster.topics.fetch(options[:name]).destroy
    end

    option :topic, type: :string
    desc "partitions", "Lists partitions"
    def partitions
      validate_class_options!

      topics = kafka_cluster.topics.values
      topics.select! { |t| t.name == options[:topic] } if options[:topic]
      topics.sort_by!(&:name)

      topics.each do |topic|
        topic.partitions.each do |partition|
          $stdout.puts "#{partition.topic.name}/#{partition.id}\tReplicas: #{partition.replicas.map(&:id).join(",")}"
        end
      end
    end

    option :replicas, type: :numeric, default: 1
    desc "critical <broker>", "Determine whether a broker is critical"
    def critical(broker_name)
      validate_class_options!

      if broker(broker_name).critical?(replicas: options[:replicas])
        raise Thor::Error, "WARNING: broker #{broker_name} is critical and cannot be stopped safely!"
      else
        $stdout.puts "Broker #{broker_name} is non-critical and can be stopped safely."
      end
    end


    desc "consumergroups", "Lists the consumergroups registered for this Kafka cluster"
    def consumergroups
      validate_class_options!

      kafka_cluster.consumergroups.each do |group|
        instances = group.instances
        if instances.length == 0
          puts "- #{group.name}: inactive"
        else
          puts "- #{group.name}: #{instances.length} running instances"
        end
      end
    end

    desc "consumergroup", "Prints information about a consumer group"
    option :name, type: :string, required: true
    def consumergroup
      validate_class_options!

      cg = Kazoo::Consumergroup.new(kafka_cluster, options[:name])
      raise Kazoo::Error, "Consumergroup #{options[:name]} is not registered in Zookeeper" unless cg.exists?

      puts "Consumer group: #{cg.name}\n"

      if cg.active?
        puts "Running instances:"
        cg.instances.each do |instance|
          puts "- #{instance.id}"
        end
      else
        puts "This consumer group is inactive."
      end
    end

    desc "delete_consumergroup", "Removes a consumer group from Zookeeper"
    option :name, type: :string, required: true
    def delete_consumergroup
      validate_class_options!

      cg = Kazoo::Consumergroup.new(kafka_cluster, options[:name])
      raise Kazoo::Error, "Consumergroup #{options[:name]} is not registered in Zookeeper" unless cg.exists?
      raise Kazoo::Error, "Cannot remove consumergroup #{cg.name} because it's still active" if cg.active?

      cg.destroy
    end


    private

    def validate_class_options!
      if options[:zookeeper].nil? || options[:zookeeper] == ''
        raise Thor::InvocationError, "Please supply --zookeeper argument, or set the ZOOKEEPER environment variable"
      end
    end

    def broker(name_or_id)
      broker = if name_or_id =~ /\A\d+\z/
        kafka_cluster.brokers[name_or_id.to_i]
      else
        kafka_cluster.brokers.values.detect { |b| b.addr == name_or_id } || cluster.brokers.values.detect { |b| b.host == name_or_id }
      end

      raise Thor::InvocationError, "Broker #{name_or_id.inspect} not found!" if broker.nil?
      broker
    end

    def kafka_cluster
      @kafka_cluster ||= Kazoo::Cluster.new(options[:zookeeper])
    end
  end
end
