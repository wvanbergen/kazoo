module Kazoo
  class CLI < Thor
    class Cluster < Thor
      include Kazoo::CLI::Common

      desc "brokers", "Describes the brokers in the cluster as registered in Zookeeper"
      def brokers
        validate_class_options!

        kafka_cluster.brokers.values.sort_by(&:id).each do |broker|
          $stdout.puts "#{broker.id}:\t#{broker.addr}\t(hosts #{broker.replicated_partitions.length} partitions, leads #{broker.led_partitions.length})"
        end
      end

      option :replicas, type: :numeric, default: 1
      desc "critical BROKER", "Determine whether a broker is critical"
      def critical(broker_name)
        validate_class_options!

        if broker(broker_name).critical?(replicas: options[:replicas])
          raise Thor::Error, "WARNING: broker #{broker_name} is critical and cannot be stopped safely!"
        else
          $stdout.puts "Broker #{broker_name} is non-critical and can be stopped safely."
        end
      end
    end
  end
end
