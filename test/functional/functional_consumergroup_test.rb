require 'test_helper'

class FunctionalConsumergroupTest < Minitest::Test
  def setup
    zookeeper = ENV["ZOOKEEPER_PEERS"] || "127.0.0.1:2181"
    @cluster = Kazoo.connect(zookeeper)
    @cg = Kazoo::Consumergroup.new(@cluster, 'test.kazoo')
    @cg.create

    @topic1 = @cluster.topic('test.1')
    @topic1.destroy if @topic1.exists?
    @topic1 = @cluster.create_topic('test.1', partitions: 1, replication_factor: 1)

    @topic4 = @cluster.topic('test.4')
    @topic4.destroy if @topic4.exists?
    @topic4 = @cluster.create_topic('test.4', partitions: 4, replication_factor: 1)
  end

  def teardown
    @topic1.destroy if @topic1.exists?
    @topic4.destroy if @topic4.exists?

    cg = Kazoo::Consumergroup.new(@cluster, 'test.kazoo')
    cg.destroy if cg.exists?

    @cluster.close
  end

  def test_create_and_destroy_consumergroup
    cg = Kazoo::Consumergroup.new(@cluster, 'test.kazoo.2')
    refute cg.exists?

    cg.create
    assert cg.exists?

    cg.destroy
    refute cg.exists?
  end

  def test_unclaimed_partitions
    partition40 = @topic4.partition(0)
    partition41 = @topic4.partition(1)
    partition42 = @topic4.partition(2)
    partition43 = @topic4.partition(3)

    refute @cg.active?

    subscription = Kazoo::Subscription.build(@topic4)
    instance1 = @cg.instantiate(subscription: subscription)
    instance1.register
    instance2 = @cg.instantiate(subscription: subscription)
    instance2.register

    assert @cg.active?

    instance1.claim_partition(partition40)
    instance2.claim_partition(partition41)

    assert_equal 2, @cg.partition_claims.length
    assert_equal [@topic4], @cg.topics
    assert_equal @topic4.partitions, @cg.partitions

    assert_equal Set[partition42, partition43], Set.new(@cg.unclaimed_partitions)
  end

  def test_watch_instances
    instance1 = @cg.instantiate(subscription: @topic1).register
    instance2 = @cg.instantiate(subscription: @topic1).register

    t = Thread.current
    instances, cb = @cg.watch_instances { t.run if t.status == 'sleep' }
    assert_equal Set[instance1, instance2], Set.new(instances)

    Thread.new { instance2.deregister }

    Thread.stop unless cb.completed?

    assert assert_equal Set[instance1], Set.new(@cg.instances)
    instance1.deregister
  end

  def test_cleanup_topics
    deleted_topic = @cluster.topic('non_existing')
    deleted_topic.destroy if deleted_topic.exists?
    deleted_topic = @cluster.create_topic('non_existing', partitions: 1, replication_factor: 1)

    subscription = Kazoo::Subscription.build([@topic4, deleted_topic])
    @cg.instantiate(subscription: subscription).register

    deleted_topic.destroy

    assert_equal Set[@topic4, deleted_topic], Set.new(@cg.topics)

    @cg.cleanup_topics(Kazoo::Subscription.everything)
    assert_equal Set[@topic4], Set.new(@cg.topics)

    @cg.cleanup_topics(Kazoo::Subscription.build([]))
    assert_equal [], @cg.topics
  end

  def test_retrieve_and_commit_offsets
    partition = Kazoo::Partition.new(@topic1, 0)

    assert_nil @cg.retrieve_offset(partition)

    @cg.commit_offset(partition, 1234)

    assert_equal 1234 + 1, @cg.retrieve_offset(partition)

    @cg.reset_all_offsets
    assert_nil @cg.retrieve_offset(partition)
  end

  def test_retrieve_offsets_and_reset_all_offsets
    partition10 = Kazoo::Partition.new(@topic1, 0)
    partition40 = Kazoo::Partition.new(@topic4, 0)
    partition41 = Kazoo::Partition.new(@topic4, 1)
    partition42 = Kazoo::Partition.new(@topic4, 2)
    partition43 = Kazoo::Partition.new(@topic4, 3)

    assert @cg.retrieve_offsets(Kazoo::Subscription.everything).values.all? { |v| v.nil? }

    @cg.commit_offset(partition10, 10)
    @cg.commit_offset(partition40, 40)
    @cg.commit_offset(partition41, 41)
    @cg.commit_offset(partition42, 42)

    offsets = @cg.retrieve_offsets(Kazoo::Subscription.everything)

    assert offsets.length >= 5
    assert_equal 11, offsets[partition10]
    assert_equal 41, offsets[partition40]
    assert_equal 42, offsets[partition41]
    assert_equal 43, offsets[partition42]
    assert_equal nil, offsets[partition43]

    offsets = @cg.retrieve_offsets(['test.1'])

    assert_equal 1, offsets.length
    assert_equal 11, offsets[partition10]

    @cg.reset_all_offsets
    assert @cg.retrieve_offsets(Kazoo::Subscription.everything).values.all? { |v| v.nil? }
  end

  def test_retrieve_all_offsets_and_cleanup_offsets
    deleted_topic = @cluster.topic('non_existing')
    deleted_topic.destroy if deleted_topic.exists?
    deleted_topic = @cluster.create_topic('non_existing', partitions: 1, replication_factor: 1)

    subscription = Kazoo::Subscription.build([@topic4, deleted_topic])

    @cg.commit_offset(@topic4.partition(0), 1234)
    @cg.commit_offset(deleted_topic.partition(0), 1234)

    deleted_topic.destroy

    expected_offsets = {
      deleted_topic.partition(0) => 1234 + 1,
      @topic4.partition(0) => 1234 + 1,
    }
    assert_equal expected_offsets, @cg.retrieve_all_offsets

    @cg.cleanup_offsets(Kazoo::Subscription.everything)

    expected_offsets = {
      @topic4.partition(0) => 1234 + 1,
    }
    assert_equal expected_offsets, @cg.retrieve_all_offsets

    @cg.cleanup_offsets([])
    assert_equal Hash.new, @cg.retrieve_all_offsets
  end
end
