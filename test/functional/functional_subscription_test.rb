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

    subscription_1  = Kazoo::Subscription.create('topic.1')
    subscription_14 = Kazoo::Subscription.create(['topic.1', 'topic.4'])

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
end
