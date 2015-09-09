require 'test_helper'

class FunctionalTopicManagementTest < Minitest::Test
  def setup
    zookeeper = ENV["ZOOKEEPER_PEERS"] || "127.0.0.1:2181"
    @cluster = Kazoo.connect(zookeeper)
  end

  def test_create_and_delete_topic
    topic = @cluster.create_topic('test.kazoo', partitions: 8, replication_factor: 1)

    assert @cluster.topics.key?(topic.name)
    assert topic.partitions.all? { |partition| @cluster.brokers.values.include?(partition.leader) }
    assert_equal 8, topic.partitions.length

    topic.destroy
    @cluster.reset_metadata

    refute topic.exists?
    refute @cluster.topics.key?(topic.name)
  end

  def test_topic_config_management
    topic = @cluster.create_topic('test.kazoo.config', partitions: 1, replication_factor: 1)

    topic.write_config("flush.messages" => 1, "max.message.bytes" => 64000)
    assert_equal "1", topic.config["flush.messages"]
    assert_equal "64000", topic.config["max.message.bytes"]

    topic.set_config("max.message.bytes", 128000)
    topic.delete_config("flush.messages")
    assert_equal topic.config, { "max.message.bytes" => "128000" }

    topic.reset_default_config
    assert_equal Hash.new, topic.config

    topic.destroy
  end
end
