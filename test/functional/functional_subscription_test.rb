require 'test_helper'

class FunctionalTopicManagementTest < Minitest::Test
  def setup
    zookeeper = ENV["ZOOKEEPER_PEERS"] || "127.0.0.1:2181"
    @cluster = Kazoo.connect(zookeeper)
  end

  def test_consumergroup_subscription
    cg = @cluster.consumergroup('test.consumergroup.subscription')
    cg.create

    assert_raises(Kazoo::NoRunningInstances) { cg.subscription }

    subscription_1  = Kazoo::Subscription.build('topic.1')
    subscription_14 = Kazoo::Subscription.build(['topic.1', 'topic.4'])

    instance1 = cg.instantiate(subscription: subscription_1).register
    instance2 = cg.instantiate(subscription: subscription_1).register
    instance3 = cg.instantiate(subscription: subscription_14).register

    assert_raises(Kazoo::InconsistentSubscriptions) { cg.subscription }

    instance3.deregister

    assert_equal subscription_1, cg.subscription
    assert_equal subscription_1, instance1.subscription

    instance2.deregister

    assert_equal subscription_1, cg.subscription
    assert_equal subscription_1, instance1.subscription

    instance1.deregister

    assert_raises(Kazoo::NoRunningInstances) { cg.subscription }
  ensure
    cg.destroy if cg.exists?
  end

  def test_subscription_topics_and_partitions
    topic1 = @cluster.create_topic('test.kazoo.subscription.1', partitions: 1, replication_factor: 1)
    topic2 = @cluster.create_topic('test.kazoo.subscription.2', partitions: 2, replication_factor: 1)
    topic3 = @cluster.create_topic('test.kazoo.non_matching', partitions: 1, replication_factor: 1)

    subscription = Kazoo::Subscription.build(/^test\.kazoo\.subscription\.\d+/)
    assert_equal Set[topic1, topic2], Set.new(subscription.topics(@cluster))
    assert_equal Set[topic1.partition(0), topic2.partition(0), topic2.partition(1)], Set.new(subscription.partitions(@cluster))

    topic2.destroy

    assert_equal Set[topic1], Set.new(subscription.topics(@cluster))
    assert_equal Set[topic1.partition(0)], Set.new(subscription.partitions(@cluster))

    topic1.destroy

    assert_equal Set[], Set.new(subscription.topics(@cluster))
    assert_equal Set[], Set.new(subscription.partitions(@cluster))

    topic3.destroy
  end
end
