module Kazoo
  class CLI < Thor
    module Common
      def self.included(base)
        base.class_option :zookeeper, type: :string, default: ENV['ZOOKEEPER']
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
          kafka_cluster.brokers.values.detect { |b| b.addr == name_or_id } || kafka_cluster.brokers.values.detect { |b| b.host == name_or_id }
        end

        raise Thor::InvocationError, "Broker #{name_or_id.inspect} not found!" if broker.nil?
        broker
      end

      def kafka_cluster
        @kafka_cluster ||= Kazoo::Cluster.new(options[:zookeeper])
      end
    end
  end
end
