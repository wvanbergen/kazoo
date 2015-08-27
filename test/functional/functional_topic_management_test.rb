require 'test_helper'

class FunctionalTopicManagementTest < Minitest::Test
  def setup
    zookeeper = ENV["ZOOKEEPER_PEERS"] || "127.0.0.1:2181"
    @cluster = Kazoo::Cluster.new(zookeeper)
  end

  def test_create_and_delete_topic
    topic = @cluster.create_topic('test.kazoo', partitions: 8, replication_factor: 1)

    assert topic.partitions.all? { |partition| @cluster.brokers.values.include?(partition.leader) }
    assert_equal 8, topic.partitions.length

  ensure
    topic = Kazoo::Topic.new(@cluster, 'test.kazoo')
    topic.destroy
    refute topic.exists?
  end
end
