module Kazoo
  # Helper class to assign replicas to brokers for new partitions.
  # It tries to e=venly divide the number of leaders and replicas over the brokers,
  # in order to get a comparable load on all the brokers in the cluster.
  class ReplicaAssigner
    attr_reader :cluster
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
      broker_leaders.values.inject(0, &:+)
    end

    def cluster_replica_count
      broker_replicas.values.inject(0, &:+)
    end

    def assign(replication_factor)
      raise Kazoo::ValidationError, "replication_factor should be higher than 0 " if replication_factor <= 0
      raise Kazoo::ValidationError, "replication_factor should not be higher than the number of brokers " if replication_factor > brokers.length

      # Order all brokers by the current number of leaders (ascending).
      # The first one will be the leader replica
      leader = @broker_leaders
        .to_a
        .sort_by { |pair| [pair[1], pair[0].id] }
        .map(&:first)
        .first

      # Update the current broker replica counts.
      # The assigned leader replica counts as a leader, but as a replica as well.
      @broker_leaders[leader] += 1
      @broker_replicas[leader] += 1

      # To assign the other replcias, we remove the broker that was selected as leader from
      # the list of brokers, and sort the rest by the number of replicas they are currently hosting.
      # Then, we take the number of remaining replcias to complete the replication factor.
      other_replicas = @broker_replicas
        .to_a
        .reject { |(key, _)| key == leader }
        .sort_by { |pair| [pair[1], pair[0].id] }
        .map(&:first)
        .take(replication_factor - 1)

      # Update the current broker replica counts.
      other_replicas.each { |broker| @broker_replicas[broker] += 1 }

      [leader].concat(other_replicas)
    end
  end
end
