require 'test_helper'

class FunctionalTopicManagementTest < Minitest::Test
  include CleanTopicSlate

  def setup
    zookeeper = ENV["ZOOKEEPER_PEERS"] || "127.0.0.1:2181"
    @cluster = Kazoo.connect(zookeeper)

    ensure_topics_do_not_exist(
      'test.kazoo',
      'test.kazoo.config',
      'test.kazoo.topic_watch',
      'test.kazoo.partition_watch.1',
      'test.kazoo.partition_watch.2',
    )
  end

  def test_create_and_delete_topic
    topic = @cluster.create_topic('test.kazoo', partitions: 8, replication_factor: 1)

    assert @cluster.topics.key?(topic.name)
    assert topic.partitions.all? { |partition| @cluster.brokers.values.include?(partition.leader) }
    assert_equal 8, topic.partitions.length

    topic.destroy

    refute topic.exists?
    refute @cluster.topics.key?(topic.name)
  end

  def test_adding_partitions_to_topic
    topic = @cluster.create_topic('test.kazoo', partitions: 2, replication_factor: 1)

    topic.add_partitions(partitions: 2, replication_factor: 1)

    assert topic.partitions.all? { |partition| @cluster.brokers.values.include?(partition.leader) }
    assert_equal 4, topic.partitions.length

    @cluster.reset_metadata

    assert topic.partitions.all? { |partition| @cluster.brokers.values.include?(partition.leader) }
    assert_equal 4, topic.partitions.length

    topic.destroy

    refute topic.exists?
    refute @cluster.topics.key?(topic.name)
  end

  def test_topic_config_management
    topic = @cluster.create_topic('test.kazoo.config', partitions: 1, replication_factor: 1, config: { "flush.messages" => 1, "max.message.bytes" => 64000 })

    assert_equal "1", topic.config["flush.messages"]
    assert_equal "64000", topic.config["max.message.bytes"]

    topic.set_config("max.message.bytes", 128000)
    topic.delete_config("flush.messages")
    assert_equal topic.config, { "max.message.bytes" => "128000" }

    topic.reset_default_config
    assert_equal Hash.new, topic.config

    topic.destroy
  end

  def test_watch_topics
    watch_fired = @cluster.watch_topics do |topics|
      refute topics.keys.include?('test.kazoo.topic_watch')
      Thread.new { @cluster.create_topic('test.kazoo.topic_watch', partitions: 1, replication_factor: 1) }
    end
    assert watch_fired

    watch_fired = @cluster.watch_topics do |topics|
      assert topics.keys.include?('test.kazoo.topic_watch')
      Thread.new { @cluster.topic('test.kazoo.topic_watch').destroy }
    end
    assert watch_fired

    refute @cluster.topic('test.kazoo.topic_watch').exists?
  end

  def test_watch_topics_with_timeout
    topic = @cluster.create_topic('test.kazoo.watch_topics', partitions: 1, replication_factor: 1)
    watch_fired = @cluster.watch_topics(timeout: 0.3) do |topics|
      Thread.new { topic.add_partitions(partitions: 2, replication_factor: 1) }
    end
    refute watch_fired

    topic.destroy
  end

  def test_watch_partitions
    topic = @cluster.create_topic('test.kazoo.partition_watch.1', partitions: 1, replication_factor: 1)

    watch_fired = topic.watch_partitions do |partitions|
      assert_equal 1, partitions.length
      Thread.new { topic.add_partitions(partitions: 2, replication_factor: 1) }
    end
    assert watch_fired

    assert_equal 3, topic.partitions.length

    watch_fired = topic.watch_partitions do |partitions|
      assert_equal 3, partitions.length
      Thread.new { topic.destroy }
    end
    assert watch_fired

    refute topic.exists?
  end

  def test_watch_partitions_for_multiple_topics
    topic1 = @cluster.create_topic('test.kazoo.partition_watch.1', partitions: 1, replication_factor: 1)
    topic2 = @cluster.create_topic('test.kazoo.partition_watch.2', partitions: 1, replication_factor: 1)

    cv = ConditionVariable.new

    thread1 = Thread.new do
      fired = topic1.watch_partitions(cv: cv) do |partitions|
        assert_equal 1, partitions.length
      end
      assert fired
    end

    thread2 = Thread.new do
      fired = topic2.watch_partitions(cv: cv) do |partitions|
        assert_equal 1, partitions.length
      end
      refute fired
    end

    Thread.new { topic1.destroy }

    thread1.join
    thread2.join

    topic2.destroy
  end
end
