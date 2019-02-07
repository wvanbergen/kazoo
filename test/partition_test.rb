require 'test_helper'

class PartitionTest < Minitest::Test
  include MockCluster

  def setup
    @cluster = mock_cluster
  end

  def test_replication_factor
    assert_equal 2, @cluster.topics['test.1'].partitions[0].replication_factor
  end

  def test_state
    partition = @cluster.topics['test.1'].partitions[0]
    partition.unstub(:leader)
    partition.unstub(:isr)

    json_payload = '{"controller_epoch":157,"leader":1,"version":1,"leader_epoch":8,"isr":[3,2,1]}'
    @cluster.zk.expects(:get).with(path: "/brokers/topics/test.1/partitions/0/state").returns(data: json_payload, rc: 0)
    @cluster.zk.expects(:get).with(path: "/brokers/topics/test.1/partitions/0/state").returns(data: json_payload, rc: 0)

    assert_equal 1, partition.leader.id
    assert_equal [3,2,1], partition.isr.map(&:id)
  end

  def test_inspect
    assert_equal "#<Kazoo::Partition test.1/0>", @cluster.topics['test.1'].partition(0).inspect
  end

  def test_equality
    topic = @cluster.topics['test.1']
    p1 = topic.partition(0)
    p2 = Kazoo::Partition.new(topic, 0)

    assert_equal p1, p2
    assert p1 != Kazoo::Partition.new(topic, 1)
    assert p1 != Kazoo::Partition.new(@cluster.topics['test.4'], 0)
    assert_equal p1.hash, p2.hash
  end

  def test_validate
    partition = Kazoo::Partition.new(@cluster.topics['test.1'], 1, replicas: [@cluster.brokers[1], @cluster.brokers[1]])
    refute partition.valid?
    assert_raises(Kazoo::ValidationError) { partition.validate }
  end

  def test_raises_unknown_broker
    partition = @cluster.topics['test.1'].partitions[0]
    partition.unstub(:leader)
    partition.unstub(:isr)

    json_payload = '{"controller_epoch":157,"leader":1,"version":1,"leader_epoch":8,"isr":[4,2,1]}'
    @cluster.zk.expects(:get).with(path: "/brokers/topics/test.1/partitions/0/state").returns(data: json_payload, rc: 0)

    assert_raises(Kazoo::Error) { partition.under_replicated? }
  end

  def test_raises_unknown_leader
    partition = @cluster.topics['test.1'].partitions[0]
    partition.unstub(:leader)
    partition.unstub(:isr)

    json_payload = '{"controller_epoch":90,"leader":-1,"version":1,"leader_epoch":135,"isr":[2,1,0]}'
    @cluster.zk.expects(:get).with(path: "/brokers/topics/test.1/partitions/0/state").returns(data: json_payload, rc: 0)

    assert_raises(Kazoo::Error) { partition.under_replicated? }
  end
end
