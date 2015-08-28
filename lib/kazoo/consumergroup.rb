module Kazoo
  class Consumergroup
    attr_reader :cluster, :name

    def initialize(cluster, name)
      @cluster, @name = cluster, name
    end

    def create
      cluster.send(:recursive_create, path: "/consumers/#{name}/ids")
      cluster.send(:recursive_create, path: "/consumers/#{name}/owners")
    end

    def destroy
      cluster.send(:recursive_delete, path: "/consumers/#{name}")
    end

    def exists?
      stat = cluster.zk.stat(path: "/consumers/#{name}")
      stat.fetch(:stat).exists?
    end


    def instantiate(id: nil)
      Instance.new(self, id: id)
    end

    def active?
      instances.length > 0
    end

    def instances
      result = cluster.zk.get_children(path: "/consumers/#{name}/ids")
      case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        result.fetch(:children).map { |id| Instance.new(self, id: id) }
      when Zookeeper::Constants::ZNONODE
        []
      else
        raise Kazoo::Error, "Failed getting a list of runniong instances for #{name}. Error code: #{result.fetch(:rc)}"
      end
    end

    def watch_instances(&block)
      cb = Zookeeper::Callbacks::WatcherCallback.create(&block)
      result = cluster.zk.get_children(path: "/consumers/#{name}/ids", watcher: cb)

      if result.fetch(:rc) != Zookeeper::Constants::ZOK
        raise Kazoo::Error, "Failed to watch instances. Error code: #{result.fetch(:rc)}"
      end

      instances = result.fetch(:children).map { |id| Instance.new(self, id: id) }
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

    def topics
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
        else raise Kazoo::Error, "Failed to retrieve offset for partition #{partition.topic.name}/#{partition.id}. Error code: #{topic_result.fetch(:rc)}"
      end

      offsets, threads, mutex = {}, [], Mutex.new
      topic_result.fetch(:children).each do |topic_name|
        threads << Thread.new do
          Thread.abort_on_exception = true

          topic = Kazoo::Topic.new(cluster, topic_name)
          partition_result = cluster.zk.get_children(path: "/consumers/#{name}/offsets/#{topic.name}")
          raise Kazoo::Error, "Failed to retrieve offsets. Error code: #{partition_result.fetch(:rc)}" if partition_result.fetch(:rc) != Zookeeper::Constants::ZOK

          partition_threads = []
          partition_result.fetch(:children).each do |partition_id|
            partition_threads << Thread.new do
              Thread.abort_on_exception = true

              partition = topic.partition(partition_id.to_i)
              offset_result = cluster.zk.get(path: "/consumers/#{name}/offsets/#{topic.name}/#{partition.id}")
              raise Kazoo::Error, "Failed to retrieve offsets. Error code: #{offset_result.fetch(:rc)}" if offset_result.fetch(:rc) != Zookeeper::Constants::ZOK

              mutex.synchronize { offsets[partition] = offset_result.fetch(:data).to_i }
            end
          end
          partition_threads.each(&:join)
        end
      end

      threads.each(&:join)
      return offsets
    end

    def commit_offset(partition, offset)
      partition_offset_path = "/consumers/#{name}/offsets/#{partition.topic.name}/#{partition.id}"
      next_offset_data = (offset + 1).to_s

      result = cluster.zk.set(path: partition_offset_path, data: next_offset_data)
      if result.fetch(:rc) == Zookeeper::Constants::ZNONODE
        cluster.send(:recursive_create, path: File.dirname(partition_offset_path))
        result = cluster.zk.create(path: partition_offset_path, data: next_offset_data)
      end

      if result.fetch(:rc) != Zookeeper::Constants::ZOK
        raise Kazoo::Error, "Failed to commit offset #{offset} for partition #{partition.topic.name}/#{partition.id}. Error code: #{result.fetch(:rc)}"
      end
    end

    def reset_all_offsets
      cluster.send(:recursive_delete, path: "/consumers/#{name}/offsets")
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

    class Instance

      def self.generate_id
        "#{Socket.gethostname}:#{SecureRandom.uuid}"
      end

      attr_reader :group, :id

      def initialize(group, id: nil)
        @group = group
        @id = id || self.class.generate_id
      end

      def registered?
        stat = cluster.zk.stat(path: "/consumers/#{group.name}/ids/#{id}")
        stat.fetch(:stat).exists?
      end

      def register(subscription)
        result = cluster.zk.create(
          path: "/consumers/#{group.name}/ids/#{id}",
          ephemeral: true,
          data: JSON.generate({
            version: 1,
            timestamp: Time.now.to_i,
            pattern: "static",
            subscription: Hash[*subscription.flat_map { |topic| [topic.name, 1] } ]
          })
        )

        if result.fetch(:rc) != Zookeeper::Constants::ZOK
          raise Kazoo::ConsumerInstanceRegistrationFailed, "Failed to register instance #{id} for consumer group #{group.name}! Error code: #{result.fetch(:rc)}"
        end

        subscription.each do |topic|
          stat = cluster.zk.stat(path: "/consumers/#{group.name}/owners/#{topic.name}")
          unless stat.fetch(:stat).exists?
            result = cluster.zk.create(path: "/consumers/#{group.name}/owners/#{topic.name}")
            if result.fetch(:rc) != Zookeeper::Constants::ZOK
              raise Kazoo::ConsumerInstanceRegistrationFailed, "Failed to register subscription of #{topic.name} for consumer group #{group.name}! Error code: #{result.fetch(:rc)}"
            end
          end
        end
      end

      def deregister
        cluster.zk.delete(path: "/consumers/#{group.name}/ids/#{id}")
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
