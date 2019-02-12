# vim: ai ts=2 sts=2 et sw=2 ft=ruby
# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=ruby

module Kazoo

  # Kazoo::Cluster represents a full Kafka cluster, based on how it is registered in Zookeeper.
  # It allows you the inspect the brokers of the cluster, the topics and partition metadata,
  # and the consumergroups that are registered against the cluster.
  class Cluster
    attr_reader :zookeeper

    def initialize(zookeeper)
      @zookeeper = zookeeper
      @zk_mutex, @brokers_mutex, @topics_mutex = Mutex.new, Mutex.new, Mutex.new
      @brokers = {}

      # TODO: Handle case where /brokers/ids does not exist

      @root = '/brokers/ids'
      children_watch.call(path: @root)
    end

    # TODO: Handle chroots?
    # Returns a zookeeper connection
    def zk
      @zk_mutex.synchronize do
        @zk ||= Zookeeper.new(zookeeper)
      end
    end

    # Returns a hash of all the brokers in the cluster
    def brokers
      @brokers_mutex.synchronize do
        @brokers
      end
    end

    # Returns a list of consumer groups that are registered against the Kafka cluster.
    def consumergroups
      @consumergroups ||= begin
        consumers = zk.get_children(path: "/consumers")
        consumers.fetch(:children).map { |name| Kazoo::Consumergroup.new(self, name) }
      end
    end

    # Returns a Kazoo::Consumergroup instance for a given consumer name.
    #
    # Note that this doesn't register a new consumer group in Zookeeper; you wil have to call
    # Kazoo::Consumergroup.create to do that.
    def consumergroup(name)
      Kazoo::Consumergroup.new(self, name)
    end

    # Returns a hash of all the topics in the Kafka cluster, indexed by the topic name.
    def topics(preload: Kazoo::Topic::DEFAULT_PRELOAD_METHODS)
      @topics_mutex.synchronize do
        @topics ||= begin
          topics = zk.get_children(path: "/brokers/topics")
          raise Kazoo::Error, "Failed to list topics. Error code: #{topics.fetch(:rc)}" unless topics.fetch(:rc) == Zookeeper::Constants::ZOK
          preload_topics_from_names(topics.fetch(:children), preload: preload)
        end
      end
    end

    # Returns a Kazoo::Topic for a given topic name.
    def topic(name)
      Kazoo::Topic.new(self, name)
    end

    # Creates a topic on the Kafka cluster, with the provided number of partitions and
    # replication factor.
    def create_topic(name, partitions: nil, replication_factor: nil, config: nil)
      raise ArgumentError, "partitions must be a positive integer" if Integer(partitions) <= 0
      raise ArgumentError, "replication_factor must be a positive integer" if Integer(replication_factor) <= 0

      Kazoo::Topic.create(self, name, partitions: Integer(partitions), replication_factor: Integer(replication_factor), config: config)
    end

    # Returns a list of all partitions hosted by the cluster
    def partitions
      topics.values.flat_map(&:partitions)
    end

    # Resets the locally cached list of brokers and topics, which will mean they will be fetched
    # freshly from Zookeeper the next time they are requested.
    def reset_metadata
      @topics, @consumergroups = nil, nil
    end

    # Returns true if any of the partitions hosted by the cluster
    def under_replicated?
      partitions.any?(&:under_replicated?)
    end

    # Triggers a preferred leader elections for the provided list of partitions. If no list of
    # partitions is provided, the preferred leader will be elected for all partitions in the cluster.
    def preferred_leader_election(partitions: nil)
      partitions = self.partitions if partitions.nil?
      result = zk.create(path: "/admin/preferred_replica_election", data: JSON.generate(version: 1, partitions: partitions))
      case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        return true
      when Zookeeper::Constants::ZNODEEXISTS
        raise Kazoo::Error, "Another preferred leader election is still in progress"
      else
        raise Kazoo::Error, "Failed to start preferred leadership election. Result code: #{result.fetch(:rc)}"
      end
    end

    # Closes the zookeeper connection and clears all the local caches.
    def close
      zk.close
      @zk = nil
      reset_metadata
    end

    protected

    # Recursively creates a node in Zookeeper, by recusrively trying to create its
    # parent if it doesn not yet exist.
    def recursive_create(path: nil)
      raise ArgumentError, "path is a required argument" if path.nil?

      result = zk.stat(path: path)
      case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        return
      when Zookeeper::Constants::ZNONODE
        recursive_create(path: File.dirname(path))
        result = zk.create(path: path)

        case result.fetch(:rc)
        when Zookeeper::Constants::ZOK, Zookeeper::Constants::ZNODEEXISTS
          return
        else
          raise Kazoo::Error, "Failed to create node #{path}. Result code: #{result.fetch(:rc)}"
        end
      else
        raise Kazoo::Error, "Failed to create node #{path}. Result code: #{result.fetch(:rc)}"
      end
    end

    # Deletes a node and all of its children from Zookeeper.
    def recursive_delete(path: nil)
      raise ArgumentError, "path is a required argument" if path.nil?

      result = zk.get_children(path: path)
      raise Kazoo::Error, "Failed to list children of #{path} to delete them. Result code: #{result.fetch(:rc)}" if result.fetch(:rc) != Zookeeper::Constants::ZOK

      threads = result.fetch(:children).map do |name|
        Thread.new do
          Thread.abort_on_exception = true
          recursive_delete(path: File.join(path, name))
        end
      end
      threads.each(&:join)

      result = zk.delete(path: path)
      raise Kazoo::Error, "Failed to delete node #{path}. Result code: #{result.fetch(:rc)}" if result.fetch(:rc) != Zookeeper::Constants::ZOK
    end

    private

    def preload_topics_from_names(names, preload: Kazoo::Topic::DEFAULT_PRELOAD_METHODS)
      result, mutex = {}, Mutex.new
      threads = names.map do |name|
        Thread.new do
          Thread.abort_on_exception = true
          topic = topic(name)
          (preload & Kazoo::Topic::ALL_PRELOAD_METHODS).each { |method| topic.send(method) }
          mutex.synchronize { result[name] = topic }
        end
      end
      threads.each(&:join)
      result
    end

    def children_watch
      Zookeeper::Callbacks::WatcherCallback.create do |cb|
        @brokers_mutex.synchronize do
          new_children = zk.get_children(path: cb.path, watcher: children_watch)

          removed_children = @brokers.keys - new_children.fetch(:children).map(&:to_i)
          removed_children.each do |child|
            @brokers.delete(child)
          end

          added_children = new_children.fetch(:children).map(&:to_i) - @brokers.keys
          added_children.map(&:to_i).each do |child|
            data = zk.get(path: "#{@root}/#{child}", watcher: data_watch(child))
            @brokers[child] = Kazoo::Broker.from_json(self, child, JSON.parse(data.fetch(:data)))
          end
        end
      end
    end

    def data_watch(child)
      Zookeeper::Callbacks::WatcherCallback.create do |cb|
        @brokers_mutex.synchronize do
          if cb.type == Zookeeper::Constants::ZOO_DELETED_EVENT
            @brokers.delete(child)
          else
            data = zk.get(path: cb.path, watcher: data_watch(child))
            @brokers[child] = Kazoo::Broker.from_json(self, child, JSON.parse(data.fetch(:data)))
          end
        end
      end
    end
  end
end
