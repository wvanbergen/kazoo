module Kazoo
  class Subscription
    attr_reader :timestamp, :version

    def self.everything
      create(/.*/)
    end

    def self.create(subscription, pattern: :white_list, timestamp: Time.now)
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

    def self.topic_name(topic)
      case topic
        when String, Symbol; topic.to_s
        when Kazoo::Topic;   topic.name
        else raise ArgumentError, "Cannot get topic name from #{topic.inspect}"
      end
    end

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

    def initialize(timestamp: Time.now, version: 1)
      @timestamp, @version = timestamp, version
    end

    def watch_partitions(kazoo)
      raise NotImplementedError
    end

    def topics(kazoo)
      kazoo.topics.values.select { |topic| has_topic?(topic) }
    end

    def has_topic?(topic)
      raise NotImplementedError
    end

    def as_json(options = {})
      {
        version:      version,
        pattern:      pattern,
        timestamp:    msec_timestamp,
        subscription: subscription,
      }
    end

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

    def subscription
      raise NotImplementedError
    end

    def pattern
      raise NotImplementedError
    end

    private

    def msec_timestamp
      (timestamp.to_i * 1000) + (timestamp.nsec / 1_000_000)
    end
  end

  class StaticSubscription < Kazoo::Subscription
    attr_reader :topic_names

    def initialize(topic_names, **kwargs)
      super(**kwargs)
      @topic_names = topic_names
    end

    def watch_partitions(kazoo)
      # TODO
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

  class PatternSubscription < Kazoo::Subscription
    PATTERN_TYPES = [:black_list, :white_list].freeze

    attr_reader :pattern, :regexp

    def initialize(regexp, pattern: :white_list, **kwargs)
      super(**kwargs)
      raise ArgumentError, "#{pattern.inspect} is not a vaid pattern type" unless PATTERN_TYPES.include?(pattern)
      @regexp, @pattern = regexp, pattern
    end

    def watch_partitions(kazoo)
      # TODO
    end

    def white_list?
      pattern == :white_list
    end

    def black_list?
      pattern == :black_list
    end

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
