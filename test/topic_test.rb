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
end
