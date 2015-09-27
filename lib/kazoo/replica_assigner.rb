module Kazoo
  class ReplicaAssigner
    attr_reader :cluster, :brokers
    attr_reader :broker_leaders, :broker_replicas

    def initialize(cluster)
      @cluster = cluster
      retrieve_initial_counts
    end

    def brokers
      @cluster.brokers
    end

    def retrieve_initial_counts
      @broker_leaders, @broker_replicas = {}, {}

      @cluster.brokers.each do |_, broker|
        @broker_leaders[broker], @broker_replicas[broker] = 0, 0
      end

      cluster.partitions.each do |partition|
        @broker_leaders[partition.preferred_leader] += 1
        partition.replicas.each do |broker|
          @broker_replicas[broker] += 1
        end
      end
    end

    def cluster_leader_count
      broker_leaders.values.inject(0) { |sum, count| sum + count }
    end

    def cluster_replica_count
      broker_replicas.values.inject(0) { |sum, count| sum + count }
    end

    def assign(replication_factor)
      raise Kazoo::ValidationError, "replication_factor should be higher than 0 " if Integer(replication_factor) <= 0
      raise Kazoo::ValidationError, "replication_factor should not be higher than the number of brokers " if Integer(replication_factor) > brokers.length

      leader = @broker_leaders
        .to_a
        .sort_by { |pair| [pair[1], pair[0].id] }
        .first.first

      @broker_leaders[leader] += 1
      @broker_replicas[leader] += 1

      other_replicas = @broker_replicas
        .to_a
        .sort_by { |pair| [(pair[0] == leader ? 1 : 0), pair[1], pair[0].id] }
        .take(replication_factor - 1)
        .map(&:first)

      other_replicas.each { |broker| @broker_replicas[broker] += 1 }

      [leader].concat(other_replicas)
    end
  end
end
