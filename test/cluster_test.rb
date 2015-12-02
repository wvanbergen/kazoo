require 'test_helper'

class ClusterTest < Minitest::Test
  include MockCluster

  def setup
    @cluster = mock_cluster
  end

  def test_cluster_under_replicated?
    refute @cluster.under_replicated?

    @cluster.topics['test.4'].partitions[2].expects(:isr).returns([@cluster.brokers[1]])
    assert @cluster.under_replicated?
  end

  def test_preferred_leader_election
    @cluster.zk.stubs(:create).with do |parameters|
      payload = JSON.parse(parameters.fetch(:data))

      [
        parameters.fetch(:path) == "/admin/preferred_replica_election",
        payload.fetch('version') == 1,
        payload.fetch('partitions').length == 1,
        payload.fetch('partitions').first.fetch('topic') == 'test.1',
        payload.fetch('partitions').first.fetch('partition') == 0,
      ].all?
    end.returns(rc: Zookeeper::Constants::ZOK)

    assert @cluster.preferred_leader_election(partitions: [@cluster.topic('test.1').partition(0)])
  end


  def test_preferred_leader_election_in_progress
    @cluster.zk.stubs(:create).returns(rc: Zookeeper::Constants::ZNODEEXISTS)
    exception = assert_raises(Kazoo::Error) do
      @cluster.preferred_leader_election
    end

    assert_equal "Another preferred leader election is still in progress", exception.message
  end
end
