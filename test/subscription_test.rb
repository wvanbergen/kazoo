require 'test_helper'

class SubscriptionTest < Minitest::Test
  include MockCluster

  def setup
    @cluster = mock_cluster
  end

  def test_static_subscription_topics
    subscription = Kazoo::Subscription.build(['test.1', 'nonexisting'])
    topics = subscription.topics(@cluster)
    assert_equal Set['test.1'], Set.new(topics.map(&:name))
  end

  def test_pattern_subscription_topics
    subscription = Kazoo::Subscription.build(/^test\.\d+/, pattern: :white_list)
    topics = subscription.topics(@cluster)
    assert_equal Set['test.1', 'test.4'], Set.new(topics.map(&:name))

    subscription = Kazoo::Subscription.build(/\.4/, pattern: :black_list)
    topics = subscription.topics(@cluster)
    assert_equal Set['test.1'], Set.new(topics.map(&:name))
  end

  def test_equality
    subscription1 = Kazoo::Subscription.build(/^test\.\d+/, pattern: :white_list)
    subscription2 = Kazoo::Subscription.build(/^test\.\d+/, pattern: :white_list)
    subscription3 = Kazoo::Subscription.build(/^test\.\d+/, pattern: :black_list)
    subscription4 = Kazoo::Subscription.build(/^test\.\d*/, pattern: :white_list)
    assert subscription1 == subscription2
    refute subscription1 == subscription3
    refute subscription1 == subscription4

    subscription1 = Kazoo::Subscription.build(:'test.1')
    subscription2 = Kazoo::Subscription.build(['test.1'])
    subscription3 = Kazoo::Subscription.build(['test.1', 'test.4'])
    assert subscription1 == subscription2
    refute subscription1 == subscription3
  end

  def test_subscription_from_json
    timestamp_msec = 628232400123
    timestamp = Time.at(BigDecimal.new(timestamp_msec) / BigDecimal.new(1000))

    json_payload = JSON.generate(
      version:      1,
      timestamp:    timestamp_msec,
      pattern:      "static",
      subscription: { 'topic.1' => 1, 'topic.4' => 1 },
    )

    subscription = Kazoo::Subscription.from_json(json_payload)
    assert_kind_of Kazoo::StaticSubscription, subscription
    assert_equal timestamp, subscription.timestamp
    assert_equal 1, subscription.version
    assert_equal Set['topic.1', 'topic.4'], Set.new(subscription.topic_names)

    json_payload = JSON.generate(
      version:      1,
      timestamp:    timestamp_msec,
      pattern:      "black_list",
      subscription: { "^test\\.\\d+" => 1 },
    )

    subscription = Kazoo::Subscription.from_json(json_payload)
    assert_kind_of Kazoo::PatternSubscription, subscription
    assert_equal timestamp, subscription.timestamp
    assert_equal 1, subscription.version
    assert subscription.black_list?
    assert_equal %r{^test\.\d+}, subscription.regexp
  end

  def test_single_topic_static_subscription_json
    subscription = Kazoo::Subscription.build('topic')
    json = subscription.to_json

    parsed_subscription = JSON.parse(json)
    assert_equal 1, parsed_subscription.fetch('version')
    assert_equal 'static', parsed_subscription.fetch('pattern')
    assert_kind_of Integer, parsed_subscription.fetch('timestamp')

    assert_kind_of Hash, parsed_subscription.fetch('subscription')
    assert_equal 1, parsed_subscription.fetch('subscription').length
    assert_equal 1, parsed_subscription.fetch('subscription').fetch('topic')
  end

  def test_multi_topic_static_subscription_json
    subscription = Kazoo::Subscription.build([:topic1, :topic2])
    json = subscription.to_json

    parsed_subscription = JSON.parse(json)
    assert_equal 1, parsed_subscription.fetch('version')
    assert_equal 'static', parsed_subscription.fetch('pattern')
    assert_kind_of Integer, parsed_subscription.fetch('timestamp')

    assert_kind_of Hash, parsed_subscription.fetch('subscription')
    assert_equal 2, parsed_subscription.fetch('subscription').length
    assert_equal 1, parsed_subscription.fetch('subscription').fetch('topic1')
    assert_equal 1, parsed_subscription.fetch('subscription').fetch('topic2')
  end

  def test_whitelist_subscription_json
    subscription = Kazoo::Subscription.build(/^topic/)
    json = subscription.to_json

    parsed_subscription = JSON.parse(json)
    assert_equal 1, parsed_subscription.fetch('version')
    assert_equal 'white_list', parsed_subscription.fetch('pattern')
    assert_kind_of Integer, parsed_subscription.fetch('timestamp')

    assert_kind_of Hash, parsed_subscription.fetch('subscription')
    assert_equal 1, parsed_subscription.fetch('subscription').length

    assert_equal ["^topic"], parsed_subscription.fetch('subscription').keys
    assert_equal [1], parsed_subscription.fetch('subscription').values
  end

  def test_blacklist_subscription_json
    subscription = Kazoo::Subscription.build(/^topic/, pattern: :black_list)
    json = subscription.to_json

    parsed_subscription = JSON.parse(json)
    assert_equal 1, parsed_subscription.fetch('version')
    assert_equal 'black_list', parsed_subscription.fetch('pattern')
    assert_kind_of Integer, parsed_subscription.fetch('timestamp')

    assert_kind_of Hash, parsed_subscription.fetch('subscription')
    assert_equal 1, parsed_subscription.fetch('subscription').length

    assert_equal ["^topic"], parsed_subscription.fetch('subscription').keys
    assert_equal [1], parsed_subscription.fetch('subscription').values
  end

  def test_subscription_has_topic
    static_subscription = Kazoo::Subscription.build(%w[topic.1 topic.4], pattern: :black_list)
    assert static_subscription.has_topic?(@cluster.topic('topic.1'))
    refute static_subscription.has_topic?(@cluster.topic('test.topic'))

    whitelist_subscription = Kazoo::Subscription.build(/^topic\./, pattern: :white_list)
    assert whitelist_subscription.has_topic?(@cluster.topic('topic.1'))
    refute whitelist_subscription.has_topic?(@cluster.topic('test.topic'))

    blacklist_subscription = Kazoo::Subscription.build(/^topic\./, pattern: :black_list)
    refute blacklist_subscription.has_topic?(@cluster.topic('topic.1'))
    assert blacklist_subscription.has_topic?(@cluster.topic('test.topic'))
  end
end
