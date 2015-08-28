module Kazoo
  class CLI < Thor
    class Consumers < Thor
      include Kazoo::CLI::Common

      desc "list", "Lists the consumergroups registered for this Kafka cluster"
      def list
        validate_class_options!

        kafka_cluster.consumergroups.sort_by(&:name).each do |group|
          instances = group.instances
          if instances.length == 0
            puts "- #{group.name}: inactive"
          else
            puts "- #{group.name}: #{instances.length} running instances"
          end
        end
      end

      desc "show NAME", "Prints information about a consumer group"
      def show(name)
        validate_class_options!

        cg = kafka_cluster.consumergroup(name)
        raise Kazoo::Error, "Consumergroup #{cg.name} is not registered in Zookeeper" unless cg.exists?

        topics = cg.topics.sort_by(&:name)

        puts "Consumer name: #{cg.name}"
        puts "Created on: #{cg.created_at}"
        puts "Topics (#{topics.length}): #{topics.map(&:name).join(', ')}"

        instances = cg.instances
        if instances.length > 0

          puts
          puts "Running instances (#{instances.length}):"
          instances.each do |instance|
            puts "- #{instance.id}\t(created on #{instance.created_at})"
          end

          partition_claims = cg.partition_claims
          if partition_claims.length > 0
            partitions = partition_claims.keys.sort_by { |p| [p.topic.name, p.id] }

            puts
            puts "Partition claims (#{partition_claims.length}):"
            partitions.each do |partition|
              instance = partition_claims[partition]
              puts "- #{partition.key}: #{instance.id}"
            end
          else
            puts
            puts "WARNING: this consumer group is active but hasn't claimed any partitions"
          end

          unclaimed_partitions = (cg.partitions - partition_claims.keys).sort_by { |p| [p.topic.name, p.id] }

          if unclaimed_partitions.length > 0
            puts
            puts "WARNING: this consumergroup has #{unclaimed_partitions.length} unclaimed partitions:"
            unclaimed_partitions.each do |partition|
              puts "- #{partition.key}"
            end
          end
        else
          puts "This consumer group is inactive."
        end
      end

      desc "delete NAME", "Removes a consumer group from Zookeeper"
      def delete(name)
        validate_class_options!

        cg = kafka_cluster.consumergroup(name)
        raise Kazoo::Error, "Consumergroup #{cg.name} is not registered in Zookeeper" unless cg.exists?
        raise Kazoo::Error, "Cannot remove consumergroup #{cg.name} because it's still active" if cg.active?

        cg.destroy
      end

      desc "reset [NAME]", "Resets all the offsets stored for a consumergroup"
      def reset(name)
        validate_class_options!

        cg = kafka_cluster.consumergroup(name)
        raise Kazoo::Error, "Consumergroup #{cg.name} is not registered in Zookeeper" unless cg.exists?
        raise Kazoo::Error, "Cannot remove consumergroup #{cg.name} because it's still active" if cg.active?

        cg.reset_all_offsets
      end
    end
  end
end
