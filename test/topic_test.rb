require 'test_helper'

class TopicTest < Minitest::Test
  include MockCluster

  def setup
    @cluster = mock_cluster
  end

  def test_from_json
    json_payload = '{"version":1,"partitions":{"2":[1,2,3],"1":[3,1,2],"3":[2,3,1],"0":[3,2,1]}}'
    topic = Kazoo::Topic.from_json(@cluster, 'test.4', JSON.parse(json_payload))

    assert_equal 4, topic.partitions.length
    assert_equal [3,2,1], topic.partitions[0].replicas.map(&:id)
    assert_equal [3,1,2], topic.partitions[1].replicas.map(&:id)
    assert_equal [1,2,3], topic.partitions[2].replicas.map(&:id)
    assert_equal [2,3,1], topic.partitions[3].replicas.map(&:id)
  end

  def test_replication_factor
    json_payload = '{"version":1,"partitions":{"2":[1,2,3],"1":[3,1,2],"3":[2,3,1],"0":[3,2,1]}}'
    topic = Kazoo::Topic.from_json(@cluster, 'test.4', JSON.parse(json_payload))
    assert_equal 3, topic.replication_factor

    json_payload = '{"version":1,"partitions":{"2":[2,3],"1":[2],"3":[2,3,1],"0":[3,2,1]}}'
    topic = Kazoo::Topic.from_json(@cluster, 'test.4', JSON.parse(json_payload))
    assert_equal 1, topic.replication_factor
  end

  def tets_topic_under_replicated?
    refute @cluster.topics['test.1'].under_replicated?
    refute @cluster.topics['test.1'].partitions[0].under_replicated?

    @cluster.topics['test.1'].partitions[0].expects(:isr).returns([@cluster.brokers[1]])

    assert @cluster.topics['test.1'].partitions[0].under_replicated?
    assert @cluster.topics['test.1'].under_replicated?
  end

  def test_inspect
    assert_equal "#<Kazoo::Topic test.1>", @cluster.topics['test.1'].inspect
  end

  def test_equality
    t1 = @cluster.topics['test.1']
    t2 = Kazoo::Topic.new(@cluster, 'test.1')

    assert_equal t1, t2
    assert t1 != Kazoo::Topic.new(@cluster, 'test.2')
    assert_equal t1.hash, t2.hash
  end

  def test_validate
    t = Kazoo::Topic.new(@cluster, "normal")
    t.partitions = [t.partition(0, replicas: [@cluster.brokers[1]])]
    assert t.valid?

    t = Kazoo::Topic.new(@cluster, "invalid/character")
    t.partitions = [t.partition(0, replicas: [@cluster.brokers[1]])]
    refute t.valid?

    t = Kazoo::Topic.new(@cluster, "l#{'o' * 253}ng")
    t.partitions = [t.partition(0, replicas: [@cluster.brokers[1]])]
    refute t.valid?

    t = Kazoo::Topic.new(@cluster, "normal")
    t.partitions = [t.partition(0, replicas: [])]
    refute t.valid?
  end

  def test_sequentially_assign_partitions
    topic = Kazoo::Topic.new(@cluster, 'test.new')

    assert_raises(ArgumentError) { topic.send(:sequentially_assign_partitions, 4, 100) }

    topic.send(:sequentially_assign_partitions, 4, 3)

    assert_equal 4, topic.partitions.length
    assert_equal 3, topic.replication_factor
    assert topic.partitions.all? { |p| p.replicas.length == 3 }
    assert topic.valid?
  end

  def test_partitions_as_json
    assignment = @cluster.topics['test.1'].send(:partitions_as_json)
    assert_equal 1, assignment.length
    assert_equal [1,2], assignment[0]
  end
end
