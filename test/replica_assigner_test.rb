require 'test_helper'

class ReplicaAssignerTest < Minitest::Test
  include MockCluster

  def setup
    @cluster = mock_cluster
    @replica_assigner = Kazoo::ReplicaAssigner.new(@cluster)
  end

  def test_initial_counts
    cluster_leader_count = @cluster.partitions.length
    assert_equal cluster_leader_count, @replica_assigner.cluster_leader_count

    assert_equal 2, @replica_assigner.broker_leaders[@cluster.brokers[1]]
    assert_equal 2, @replica_assigner.broker_leaders[@cluster.brokers[2]]
    assert_equal 1, @replica_assigner.broker_leaders[@cluster.brokers[3]]

    cluster_replica_count = @cluster.partitions.inject(0) { |sum, p| sum + p.replicas.length }
    assert_equal cluster_replica_count, @replica_assigner.cluster_replica_count

    assert_equal 3, @replica_assigner.broker_replicas[@cluster.brokers[1]]
    assert_equal 4, @replica_assigner.broker_replicas[@cluster.brokers[2]]
    assert_equal 3, @replica_assigner.broker_replicas[@cluster.brokers[3]]
  end

  def test_assign_replicas_validation
    assert_raises(Kazoo::ValidationError) { @replica_assigner.assign(4) }
    assert_raises(Kazoo::ValidationError) { @replica_assigner.assign(0) }
  end

  def test_assign_replicas_deterministically
    replicas_1 = @replica_assigner.assign(2)
    assert_equal [@cluster.brokers[3], @cluster.brokers[1]], replicas_1

    assert_equal 2, @replica_assigner.broker_leaders[@cluster.brokers[1]]
    assert_equal 2, @replica_assigner.broker_leaders[@cluster.brokers[2]]
    assert_equal 2, @replica_assigner.broker_leaders[@cluster.brokers[3]]

    assert_equal 4, @replica_assigner.broker_replicas[@cluster.brokers[1]]
    assert_equal 4, @replica_assigner.broker_replicas[@cluster.brokers[2]]
    assert_equal 4, @replica_assigner.broker_replicas[@cluster.brokers[3]]

    replicas_2 = @replica_assigner.assign(3)
    assert_equal [@cluster.brokers[1], @cluster.brokers[2], @cluster.brokers[3]], replicas_2

    assert_equal 3, @replica_assigner.broker_leaders[@cluster.brokers[1]]
    assert_equal 2, @replica_assigner.broker_leaders[@cluster.brokers[2]]
    assert_equal 2, @replica_assigner.broker_leaders[@cluster.brokers[3]]

    assert_equal 5, @replica_assigner.broker_replicas[@cluster.brokers[1]]
    assert_equal 5, @replica_assigner.broker_replicas[@cluster.brokers[2]]
    assert_equal 5, @replica_assigner.broker_replicas[@cluster.brokers[3]]

  end

  def test_assign_replicas_evenly
    initial_leaders = @replica_assigner.cluster_leader_count
    initial_replicas = @replica_assigner.cluster_replica_count

    1000.times do
      replicas = @replica_assigner.assign(2)
      assert_equal 2, replicas.length
    end

    # We expect the cluster-wide counters to be increased by the expected amount of replicas
    final_leaders = @replica_assigner.cluster_leader_count
    final_replicas = @replica_assigner.cluster_replica_count

    assert_equal final_leaders, initial_leaders + 1000
    assert_equal final_replicas, initial_replicas + 2000

    # For the entire cluster, the number of leaders and replicas should be even for every broker.
    leader_diff = @replica_assigner.broker_leaders.values.max - @replica_assigner.broker_leaders.values.min
    assert leader_diff >= 0 && leader_diff <= 1

    replica_diff = @replica_assigner.broker_replicas.values.max - @replica_assigner.broker_replicas.values.min
    assert replica_diff >= 0 && replica_diff <= 1
  end
end
