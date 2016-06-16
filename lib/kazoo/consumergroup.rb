module Kazoo
  class Consumergroup
    attr_reader :cluster, :name

    def initialize(cluster, name)
      @cluster, @name = cluster, name
    end

    def create
      cluster.send(:recursive_create, path: "/consumers/#{name}/ids")
      cluster.send(:recursive_create, path: "/consumers/#{name}/owners")
      cluster.reset_metadata
    end

    def destroy
      cluster.send(:recursive_delete, path: "/consumers/#{name}")
      cluster.reset_metadata
    end

    def exists?
      stat = cluster.zk.stat(path: "/consumers/#{name}")
      stat.fetch(:stat).exists?
    end

    def created_at
      result = cluster.zk.stat(path: "/consumers/#{name}")
      raise Kazoo::Error, "Failed to get consumer details. Error code: #{result.fetch(:rc)}" if result.fetch(:rc) != Zookeeper::Constants::ZOK

      Time.at(result.fetch(:stat).mtime / 1000.0)
    end

    def instantiate(id: nil, subscription: nil)
      Instance.new(self, id: id, subscription: subscription)
    end

    def subscription
      subscriptions = instances.map(&:subscription).compact
      raise NoRunningInstances, "Consumergroup #{name} has no running instances; cannot determine subscription" if subscriptions.length == 0

      subscriptions.uniq!
      raise InconsistentSubscriptions, "Subscriptions of running instances are different from each other" if subscriptions.length != 1

      subscriptions.first
    end

    def active?
      instances.length > 0
    end

    def instances
      result = cluster.zk.get_children(path: "/consumers/#{name}/ids")
      case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        instances_with_subscription(result.fetch(:children))
      when Zookeeper::Constants::ZNONODE
        []
      else
        raise Kazoo::Error, "Failed getting a list of runniong instances for #{name}. Error code: #{result.fetch(:rc)}"
      end
    end

    def watch_instances(&block)
      cb = Zookeeper::Callbacks::WatcherCallback.create(&block)
      result = cluster.zk.get_children(path: "/consumers/#{name}/ids", watcher: cb)
      instances = case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        instances_with_subscription(result.fetch(:children))
      when Zookeeper::Constants::ZNONODE
        []
      else
        raise Kazoo::Error, "Failed getting a list of runniong instances for #{name}. Error code: #{result.fetch(:rc)}"
      end

      [instances, cb]
    end

    def watch_partition_claim(partition, &block)
      cb = Zookeeper::Callbacks::WatcherCallback.create(&block)

      result = cluster.zk.get(path: "/consumers/#{name}/owners/#{partition.topic.name}/#{partition.id}", watcher: cb)

      case result.fetch(:rc)
      when Zookeeper::Constants::ZNONODE # Nobody is claiming this partition yet
        [nil, nil]
      when Zookeeper::Constants::ZOK
        [Kazoo::Consumergroup::Instance.new(self, id: result.fetch(:data)), cb]
      else
        raise Kazoo::Error, "Failed to set watch for partition claim of #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
      end
    end

    def claimed_topics
      topic_result = cluster.zk.get_children(path: "/consumers/#{name}/owners")
      case topic_result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        topic_result.fetch(:children).map { |topic_name| cluster.topic(topic_name) }
      when Zookeeper::Constants::ZNONODE
        []
      else
        raise Kazoo::Error, "Failed to get subscribed topics. Result code: #{topic_result.fetch(:rc)}"
      end
    end

    def subscribed_topics
      subscription.topics(cluster)
    end

    alias_method :topics, :subscribed_topics

    def partitions
      partitions, threads, mutex = [], [], Mutex.new
      topics.each do |topic|
        threads << Thread.new do
          topic_partitions = topic.partitions
          mutex.synchronize { partitions.concat(topic_partitions) }
        end
      end

      threads.each(&:join)
      return partitions
    end

    def unclaimed_partitions
      partitions - partition_claims.keys
    end

    def partition_claims
      topic_result = cluster.zk.get_children(path: "/consumers/#{name}/owners")
      case topic_result.fetch(:rc)
        when Zookeeper::Constants::ZOK; # continue
        when Zookeeper::Constants::ZNONODE; return {}
        else raise Kazoo::Error, "Failed to get partition claims. Result code: #{topic_result.fetch(:rc)}"
      end

      partition_claims, threads, mutex = {}, [], Mutex.new
      topic_result.fetch(:children).each do |topic_name|
        threads << Thread.new do
          topic = cluster.topic(topic_name)

          partition_result = cluster.zk.get_children(path: "/consumers/#{name}/owners/#{topic.name}")
          raise Kazoo::Error, "Failed to get partition claims. Result code: #{partition_result.fetch(:rc)}" if partition_result.fetch(:rc) != Zookeeper::Constants::ZOK

          partition_threads = []
          partition_result.fetch(:children).each do |partition_id|
            partition_threads << Thread.new do
              partition = topic.partition(partition_id.to_i)

              claim_result =cluster.zk.get(path: "/consumers/#{name}/owners/#{topic.name}/#{partition.id}")
              raise Kazoo::Error, "Failed to get partition claims. Result code: #{claim_result.fetch(:rc)}" if claim_result.fetch(:rc) != Zookeeper::Constants::ZOK

              mutex.synchronize { partition_claims[partition] = instantiate(id: claim_result.fetch(:data)) }
            end
          end
          partition_threads.each(&:join)
        end
      end

      threads.each(&:join)
      return partition_claims
    end

    def retrieve_offset(partition)
      result = cluster.zk.get(path: "/consumers/#{name}/offsets/#{partition.topic.name}/#{partition.id}")
      case result.fetch(:rc)
        when Zookeeper::Constants::ZOK; result.fetch(:data).to_i
        when Zookeeper::Constants::ZNONODE; nil
        else raise Kazoo::Error, "Failed to retrieve offset for partition #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
      end
    end

    def retrieve_all_offsets
      topic_result = cluster.zk.get_children(path: "/consumers/#{name}/offsets")
      case topic_result.fetch(:rc)
        when Zookeeper::Constants::ZOK; # continue
        when Zookeeper::Constants::ZNONODE; return {}
        else raise Kazoo::Error, "Failed to get topic offsets. Result code: #{topic_result.fetch(:rc)}"
      end

      offsets, mutex = {}, Mutex.new
      topic_threads = topic_result.fetch(:children).map do |topic_name|
        Thread.new do
          Thread.abort_on_exception = true

          topic = cluster.topic(topic_name)
          partition_result = cluster.zk.get_children(path: "/consumers/#{name}/offsets/#{topic.name}")
          raise Kazoo::Error, "Failed to get partition offsets. Result code: #{partition_result.fetch(:rc)}" if partition_result.fetch(:rc) != Zookeeper::Constants::ZOK

          partition_threads = partition_result.fetch(:children).map do |partition_id|
            Thread.new do
              Thread.abort_on_exception = true

              partition = topic.partition(partition_id.to_i)
              offset_result = cluster.zk.get(path: "/consumers/#{name}/offsets/#{topic.name}/#{partition.id}")
              offset = case offset_result.fetch(:rc)
              when Zookeeper::Constants::ZOK
                offset_result.fetch(:data).to_i
              when Zookeeper::Constants::ZNONODE
                nil
              else
                raise Kazoo::Error, "Failed to retrieve offset for #{partition.key}. Error code: #{offset_result.fetch(:rc)}"
              end
              mutex.synchronize { offsets[partition] = offset }
            end
          end
          partition_threads.each(&:join)
        end
      end

      topic_threads.each(&:join)
      return offsets
    end

    def retrieve_offsets(subscription = self.subscription)
      subscription = Kazoo::Subscription.build(subscription)

      offsets, mutex = {}, Mutex.new
      topic_threads = subscription.topics(cluster).map do |topic|
        Thread.new do
          Thread.abort_on_exception = true

          partition_threads = topic.partitions.map do |partition|
            Thread.new do
              Thread.abort_on_exception = true

              offset_result = cluster.zk.get(path: "/consumers/#{name}/offsets/#{topic.name}/#{partition.id}")
              offset = case offset_result.fetch(:rc)
              when Zookeeper::Constants::ZOK
                offset_result.fetch(:data).to_i
              when Zookeeper::Constants::ZNONODE
                nil
              else
                raise Kazoo::Error, "Failed to retrieve offset for #{partition.key}. Error code: #{offset_result.fetch(:rc)}"
              end
              mutex.synchronize { offsets[partition] = offset }
            end
          end
          partition_threads.each(&:join)
        end
      end

      topic_threads.each(&:join)
      return offsets
    end

    def commit_offset(partition, offset)
      partition_offset_path = "/consumers/#{name}/offsets/#{partition.topic.name}/#{partition.id}"
      next_offset_data = (offset + 1).to_s

      result = cluster.zk.set(path: partition_offset_path, data: next_offset_data)
      if result.fetch(:rc) == Zookeeper::Constants::ZNONODE
        cluster.send(:recursive_create, path: partition_offset_path)
        result = cluster.zk.set(path: partition_offset_path, data: next_offset_data)
      end

      if result.fetch(:rc) != Zookeeper::Constants::ZOK
        raise Kazoo::Error, "Failed to commit offset #{offset} for partition #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
      end
    end

    def reset_all_offsets
      cluster.send(:recursive_delete, path: "/consumers/#{name}/offsets")
    end

    def clean_topic_claims(subscription = nil)
      subscription = subscription.nil? ? self.subscription : Kazoo::Subscription.build(subscription)

      threads = claimed_topics.map do |topic|
        Thread.new do
          Thread.abort_on_exception = true
          unless subscription.topics(cluster).include?(topic)
            cluster.send(:recursive_delete, path: "/consumers/#{name}/owners/#{topic.name}")
          end
        end
      end

      threads.each(&:join)
    end

    def clean_stored_offsets(subscription = nil)
      subscription = subscription.nil? ? self.subscription : Kazoo::Subscription.build(subscription)

      topics_result = cluster.zk.get_children(path: "/consumers/#{name}/offsets")
      raise Kazoo::Error, "Failed to retrieve list of topics. Error code: #{topics_result.fetch(:rc)}" if topics_result.fetch(:rc) != Zookeeper::Constants::ZOK

      threads = topics_result.fetch(:children).map do |topic_name|
        Thread.new do
          Thread.abort_on_exception = true
          topic = cluster.topic(topic_name)
          unless subscription.topics(cluster).include?(topic)
            cluster.send(:recursive_delete, path: "/consumers/#{name}/offsets/#{topic.name}")
          end
        end
      end

      threads.each(&:join)
    end

    def inspect
      "#<Kazoo::Consumergroup name=#{name}>"
    end

    def eql?(other)
      other.kind_of?(Kazoo::Consumergroup) && cluster == other.cluster && name == other.name
    end

    alias_method :==, :eql?

    def hash
      [cluster, name].hash
    end

    protected

    def instances_with_subscription(instance_ids)
      instances, threads, mutex = [], [], Mutex.new
      instance_ids.each do |id|
        threads << Thread.new do
          Thread.abort_on_exception = true

          subscription_result = cluster.zk.get(path: "/consumers/#{name}/ids/#{id}")
          raise Kazoo::Error, "Failed to retrieve subscription for instance. Error code: #{subscription_result.fetch(:rc)}" if subscription_result.fetch(:rc) != Zookeeper::Constants::ZOK
          subscription = Kazoo::Subscription.from_json(subscription_result.fetch(:data))
          mutex.synchronize { instances << Instance.new(self, id: id, subscription: subscription) }
        end
      end
      threads.each(&:join)
      instances
    end

    class Instance

      def self.generate_id
        "#{Socket.gethostname}:#{SecureRandom.uuid}"
      end

      attr_reader :group, :id, :subscription

      def initialize(group, id: nil, subscription: nil)
        @group = group
        @id = id || self.class.generate_id
        @subscription = Kazoo::Subscription.build(subscription) unless subscription.nil?
      end

      def registered?
        stat = cluster.zk.stat(path: "/consumers/#{group.name}/ids/#{id}")
        stat.fetch(:stat).exists?
      end

      def register(subscription_deprecated = nil)
        # Don't provide the subscription here, but provide it when instantiating the consumer instance.
        @subscription = Kazoo::Subscription.build(subscription_deprecated) unless subscription_deprecated.nil?

        result = cluster.zk.create(
          path: "/consumers/#{group.name}/ids/#{id}",
          ephemeral: true,
          data: subscription.to_json,
        )

        if result.fetch(:rc) != Zookeeper::Constants::ZOK
          raise Kazoo::ConsumerInstanceRegistrationFailed, "Failed to register instance #{id} for consumer group #{group.name}! Error code: #{result.fetch(:rc)}"
        end

        subscription.topics(cluster).each do |topic|
          stat = cluster.zk.stat(path: "/consumers/#{group.name}/owners/#{topic.name}")
          unless stat.fetch(:stat).exists?
            result = cluster.zk.create(path: "/consumers/#{group.name}/owners/#{topic.name}")
            if result.fetch(:rc) != Zookeeper::Constants::ZOK
              raise Kazoo::ConsumerInstanceRegistrationFailed, "Failed to register subscription of #{topic.name} for consumer group #{group.name}! Error code: #{result.fetch(:rc)}"
            end
          end
        end

        return self
      end

      def created_at
        result = cluster.zk.stat(path: "/consumers/#{group.name}/ids/#{id}")
        raise Kazoo::Error, "Failed to get instance details. Error code: #{result.fetch(:rc)}" if result.fetch(:rc) != Zookeeper::Constants::ZOK

        Time.at(result.fetch(:stat).mtime / 1000.0)
      end


      def deregister
        result = cluster.zk.delete(path: "/consumers/#{group.name}/ids/#{id}")
        if result.fetch(:rc) != Zookeeper::Constants::ZOK
          raise Kazoo::Error, "Failed to deregister instance #{id} for consumer group #{group.name}! Error code: #{result.fetch(:rc)}"
        end

        return self
      end

      def claim_partition(partition)
        result = cluster.zk.create(
          path: "/consumers/#{group.name}/owners/#{partition.topic.name}/#{partition.id}",
          ephemeral: true,
          data: id,
        )

        case result.fetch(:rc)
        when Zookeeper::Constants::ZOK
          return true
        when Zookeeper::Constants::ZNODEEXISTS
          raise Kazoo::PartitionAlreadyClaimed, "Partition #{partition.topic.name}/#{partition.id} is already claimed!"
        else
          raise Kazoo::Error, "Failed to claim partition #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
        end
      end

      def release_partition(partition)
        result = cluster.zk.delete(path: "/consumers/#{group.name}/owners/#{partition.topic.name}/#{partition.id}")
        if result.fetch(:rc) != Zookeeper::Constants::ZOK
          raise Kazoo::Error, "Failed to release partition #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
        end
      end

      def inspect
        "#<Kazoo::Consumergroup::Instance group=#{group.name} id=#{id}>"
      end

      def hash
        [group, id].hash
      end

      def eql?(other)
        other.kind_of?(Kazoo::Consumergroup::Instance) && group == other.group && id == other.id
      end

      alias_method :==, :eql?

      private

      def cluster
        group.cluster
      end
    end
  end
end
