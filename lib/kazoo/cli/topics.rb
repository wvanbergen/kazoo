module Kazoo
  class CLI < Thor
    class Topics < Thor
      include Kazoo::CLI::Common

      desc "list", "Lists all topics in the cluster"
      def list
        validate_class_options!

        kafka_cluster.topics.values.sort_by(&:name).each do |topic|
          $stdout.puts topic.name
        end
      end

      desc "create TOPIC", "Creates a new topic"
      option :partitions, type: :numeric, required: true
      option :replication_factor, type: :numeric, required: true
      def create(name)
        validate_class_options!

        kafka_cluster.create_topic(name, partitions: options[:partitions], replication_factor: options[:replication_factor])
      end

      desc "delete TOPIC", "Removes a topic"
      def delete(name)
        validate_class_options!

        kafka_cluster.topics.fetch(name).destroy
      end

      desc "partitions TOPIC", "Lists partitions for a topic"
      def partitions(topic)
        validate_class_options!

        topic = kafka_cluster.topics.fetch(topic)
        topic.partitions.each do |partition|
          puts "#{partition.key}\tReplicas: #{partition.replicas.map(&:id).join(",")}\tISR: #{partition.isr.map(&:id).join(",")}"
        end
      end

      option :partitions, type: :numeric, required: true
      option :replication_factor, type: :numeric, required: false
      desc "set_partitions TOPIC", "Lists partitions for a topic"
      def set_partitions(topic)
        validate_class_options!

        topic = kafka_cluster.topics.fetch(topic)
        new_partitions = options[:partitions] - topic.partitions.length
        raise "You can only add partitions to a topic, not remove them" if new_partitions <= 0

        replication_factor = options[:replication_factor] || topic.replication_factor
        topic.add_partitions(partitions: new_partitions, replication_factor: replication_factor)
      end
    end
  end
end
