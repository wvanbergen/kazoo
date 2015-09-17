module Kazoo

  # A Kazoo::Subscription describes interest in a set of topics of a Kafka cluster.
  #
  # Use Kazoo::Subscription.build to instantiate a subscription. It will return one of
  # the two known subclasses of Kazoo::Subscription: a Kazoo::StaticSubscription for
  # a static list of topics, or a Kazoo::PatternSUbscription for a dynamic list based
  # on a regular expression that serves as a white list or black list.
  class Subscription
    attr_reader :timestamp, :version

    # Instantiates a whitelist subscription that matches every topic.
    def self.everything
      build(/.*/)
    end

    # Instantiates a Kazoo::Subscription based on the subscription argument.
    #
    # - If the subscription argument is the name of a topic, a Kazoo::Topic, or an array of those,
    #   it will create a static subscription for the provided topic.
    # - If the subscription argument is a regular expression, it will create a pattern subscription.
    #   The `pattern` argument will determine whether it is a white_list (default), or black_list.
    # - If the subscription argument is a Kazoo::Subscription, it will return the argument itself.
    def self.build(subscription, pattern: :white_list, timestamp: Time.now)
      case subscription
      when Kazoo::Subscription
        subscription
      when String, Symbol, Kazoo::Topic, Array
        topic_names = Array(subscription).map { |t| topic_name(t) }
        Kazoo::StaticSubscription.new(topic_names, timestamp: timestamp)
      when Regexp
        Kazoo::PatternSubscription.new(subscription, pattern: pattern, timestamp: timestamp)
      else
        raise ArgumentError, "Don't know how to create a subscription from #{subscription.inspect}"
      end
    end

    # Instantiates a Kazoo::Subscription based on a JSON payload as it is stored in Zookeeper.
    #
    # This method will raise Kazoo::InvalidSubscription if the JSON payload cannot be parsed.
    # Only version 1 payloads are supported.
    def self.from_json(json_payload)
      json = JSON.parse(json_payload)
      version, timestamp = json.fetch('version'), json.fetch('timestamp')
      raise Kazoo::InvalidSubscription, "Only version 1 subscriptions are supported, found version #{version}!" unless version == 1

      time = Time.at(BigDecimal.new(timestamp) / BigDecimal.new(1000))

      pattern, subscription = json.fetch('pattern'), json.fetch('subscription')
      raise Kazoo::InvalidSubscription, "Only subscriptions with a single stream are supported" unless subscription.values.all? { |streams| streams == 1 }

      case pattern
      when 'static'
        topic_names = subscription.keys
        Kazoo::StaticSubscription.new(topic_names, version: version, timestamp: time)

      when 'white_list', 'black_list'
        raise Kazoo::InvalidSubscription, "Only pattern subscriptions with a single expression are supported" unless subscription.keys.length == 1
        regexp = Regexp.new(subscription.keys.first.tr(',', '|'))
        Kazoo::PatternSubscription.new(regexp, pattern: pattern.to_sym, version: version, timestamp: time)

      else
        raise Kazoo::InvalidSubscription, "Unrecognized subscription pattern #{pattern.inspect}"
      end

    rescue JSON::ParserError, KeyError => e
      raise Kazoo::InvalidSubscription.new(e.message)
    end


    # Returns a topic name based on various inputs.
    # Helper method used by Kazoo::Subscription.build
    def self.topic_name(topic)
      case topic
        when String, Symbol; topic.to_s
        when Kazoo::Topic;   topic.name
        else raise ArgumentError, "Cannot get topic name from #{topic.inspect}"
      end
    end

    # Returns an array of all Kazoo::Topic instances in the given Kafka cluster
    # that are matched by this subscription.
    def topics(cluster)
      cluster.topics.values.select { |topic| has_topic?(topic) }
    end

    # Returns an array of all Kazoo::Partition instances in the given Kafka cluster
    # that are matched by this subscription.
    def partitions(cluster)
      topics(cluster).flat_map { |topic| topic.partitions }
    end

    # has_topic? should return true if a given Kazoo::Topic is part of this subscription.
    def has_topic?(topic)
      raise NotImplementedError
    end

    # Returns the JSON representation of this subscription that can be stored in Zookeeper.
    def to_json(options = {})
      JSON.dump(as_json(options))
    end

    def eql?(other)
      other.kind_of?(Kazoo::Subscription) && other.pattern == pattern && other.subscription == subscription
    end

    alias_method :==, :eql?

    def hash
      [pattern, subscription].hash
    end

    def inspect
      "#<#{self.class.name} pattern=#{pattern} subscription=#{subscription.inspect}>"
    end

    protected

    # Subclasses should call super(**kwargs) in their initializer.
    def initialize(timestamp: Time.now, version: 1)
      @timestamp, @version = timestamp, version
    end

    # Subclasses should return a hash that can be converted to JSON to represent
    # the subscription in Zookeeper.
    def subscription
      raise NotImplementedError
    end

    # Should return the name of the pattern, i.e. static, white_list, or black_list
    def pattern
      raise NotImplementedError
    end

    private

    def as_json(options = {})
      {
        version:      version,
        pattern:      pattern,
        timestamp:    msec_timestamp,
        subscription: subscription,
      }
    end

    def msec_timestamp
      (timestamp.to_i * 1000) + (timestamp.nsec / 1_000_000)
    end
  end

  # StaticSubscription describes a subscription based on a static list of topic names.
  class StaticSubscription < Kazoo::Subscription
    attr_reader :topic_names

    # Instantiates a static subscription instance. The topic_names argument must be
    # an array of strings.
    def initialize(topic_names, **kwargs)
      super(**kwargs)
      @topic_names = topic_names
    end

    def has_topic?(topic)
      topic_names.include?(topic.name)
    end

    protected

    def pattern
      :static
    end

    def subscription
      topic_names.inject({}) { |hash, topic_name| hash[topic_name] = 1; hash }
    end
  end

  # PatternSubscription describes a subscription based on a regular expression that
  # serves as either a whitelist or blacklist filter for all topics available in
  # a Kafka cluster.
  class PatternSubscription < Kazoo::Subscription
    PATTERN_TYPES = [:black_list, :white_list].freeze

    attr_reader :pattern, :regexp

    def initialize(regexp, pattern: :white_list, **kwargs)
      super(**kwargs)
      raise ArgumentError, "#{pattern.inspect} is not a valid pattern type" unless PATTERN_TYPES.include?(pattern)
      @regexp, @pattern = regexp, pattern
    end

    # Returns true if this subscription uses a whitelist pattern
    def white_list?
      pattern == :white_list
    end

    # Returns true if this subscription uses a blacklist pattern
    def black_list?
      pattern == :black_list
    end

    # Returns true if the whitelist or blacklist does not filter out the provided topic.
    def has_topic?(topic)
      case pattern
        when :white_list; topic.name =~ regexp
        when :black_list; topic.name !~ regexp
      end
    end

    protected

    def subscription
      { regexp.inspect[1..-2] => 1 }
    end
  end
end
