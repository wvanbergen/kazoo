require 'test_helper'

require 'kazoo/replica_reassigner'

class ReplicaReassignerTest < Minitest::Test
  include MockCluster

  def setup
    @cluster = mock_cluster
    @replica_reassigner = Kazoo::ReplicaReassigner.new(@cluster)
  end

  def test_agony
    assert_equal 0, Kazoo::ReplicaReassigner.agony([1], [1])
    assert_equal 0, Kazoo::ReplicaReassigner.agony([1, 2], [1, 2])

    assert_equal 1, Kazoo::ReplicaReassigner.agony([1], [2])
    assert_equal 1, Kazoo::ReplicaReassigner.agony([1, 3], [3, 2])
    assert_equal 1, Kazoo::ReplicaReassigner.agony([1, 2, 3], [3, 2, 4])

    assert_equal 2, Kazoo::ReplicaReassigner.agony([1, 2, 3], [3, 4, 5])
    assert_equal 2, Kazoo::ReplicaReassigner.agony([1, 2, 3], [5, 4, 3])
  end

  def test_valid_replicaset?
    assert Kazoo::ReplicaReassigner.valid_replicaset?([1,2,3])
    assert Kazoo::ReplicaReassigner.valid_replicaset?([1,2,3], minimum_replicas: 3)

    refute Kazoo::ReplicaReassigner.valid_replicaset?([])
    refute Kazoo::ReplicaReassigner.valid_replicaset?([1,1])
    refute Kazoo::ReplicaReassigner.valid_replicaset?([1,2,3], minimum_replicas: 4)
  end

  def test_safe_reassignment?
    assert Kazoo::ReplicaReassigner.safe_reassignment?([1,2,3], [1,2,4])
    assert Kazoo::ReplicaReassigner.safe_reassignment?([1,2,3], [1,2,4], max_agony: 1)
    assert Kazoo::ReplicaReassigner.safe_reassignment?([1,2,3], [1,4,5], max_agony: 2)

    refute Kazoo::ReplicaReassigner.safe_reassignment?([1,2,3], [1,4,5])
    refute Kazoo::ReplicaReassigner.safe_reassignment?([1,2,3], [4,5,6], max_agony: 1000)
    refute Kazoo::ReplicaReassigner.safe_reassignment?([1,2,3], [1,2], minimum_replicas: 3)
  end

  def test_steps_for_full_reassignment
    steps = Kazoo::ReplicaReassigner.steps([1,2,3], [4,5,6], max_agony: 1, include_initial: true)

    assert_equal [1,2,3], steps.first
    assert_equal [4,5,6], steps.last

    steps.each_cons(2) do |from, to|
      assert Kazoo::ReplicaReassigner.safe_reassignment?(from, to, max_agony: 1, minimum_replicas: 3)
    end
  end

  def test_steps_for_partition_addition
    steps = Kazoo::ReplicaReassigner.steps([1], [4,5,6], max_agony: 1, include_initial: true)

    assert_equal [1], steps.first
    assert_equal [4,5,6], steps.last

    steps.each_cons(2) do |from, to|
      assert Kazoo::ReplicaReassigner.safe_reassignment?(from, to, max_agony: 1, minimum_replicas: 1)
    end
  end

  def test_steps_for_partition_removal
    steps = Kazoo::ReplicaReassigner.steps([4,5,6], [1], max_agony: 1, include_initial: true)

    assert_equal [4,5,6], steps.first
    assert_equal [1], steps.last

    steps.each_cons(2) do |from, to|
      assert Kazoo::ReplicaReassigner.safe_reassignment?(from, to, max_agony: 1, minimum_replicas: 1)
    end
  end
end
