module Kazoo
  class Topic
    ALL_PRELOAD_METHODS     = [:partitions, :config].freeze
    DEFAULT_PRELOAD_METHODS = [:partitions].freeze
    VALID_TOPIC_NAMES       = %r{\A[a-zA-Z0-9\\._\\-]+\z}
    BLACKLISTED_TOPIC_NAMES = %r{\A\.\.?\z}

    attr_reader :cluster, :name

    def initialize(cluster, name, config: nil, partitions: nil)
      @cluster, @name = cluster, name

      self.partitions = partitions
      self.config = config
    end

    def partitions
      @partitions ||= load_partitions_from_zookeeper
    end

    def partitions=(ps)
      @partitions = ps
    end

    def partition(index, **kwargs)
      Kazoo::Partition.new(self, index, **kwargs)
    end

    def append_partition(**kwargs)
      new_partition = partition(partitions.length, **kwargs)
      partitions << new_partition
      new_partition
    end

    def replication_factor
      partitions.map(&:replication_factor).min
    end

    def under_replicated?
      partitions.any?(&:under_replicated?)
    end

    def inspect
      "#<Kazoo::Topic #{name}>"
    end

    def eql?(other)
      other.kind_of?(Kazoo::Topic) && cluster == other.cluster && name == other.name
    end

    alias_method :==, :eql?

    def hash
      [cluster, name].hash
    end

    def exists?
      stat = cluster.zk.stat(path: "/brokers/topics/#{name}")
      stat.fetch(:stat).exists?
    end

    def validate
      raise Kazoo::ValidationError, "#{name} is not a valid topic name" if VALID_TOPIC_NAMES !~ name
      raise Kazoo::ValidationError, "#{name} is not a valid topic name" if BLACKLISTED_TOPIC_NAMES =~ name
      raise Kazoo::ValidationError, "#{name} is too long" if name.length > 255
      raise Kazoo::ValidationError, "The topic has no partitions defined" if partitions.length == 0
      partitions.each(&:validate)

      true
    end

    def valid?
      validate
    rescue Kazoo::ValidationError
      false
    end

    def add_partitions(partitions: nil, replication_factor: nil)
      raise ArgumentError, "partitions must be a positive integer" if Integer(partitions) <= 0
      raise ArgumentError, "replication_factor must be a positive integer" if Integer(replication_factor) <= 0

      raise Kazoo::TopicNotFound, "The topic #{name} does not exists!" unless exists?

      replica_assigner = Kazoo::ReplicaAssigner.new(cluster)

      partitions.times do
        replicas = replica_assigner.assign(replication_factor)
        append_partition(replicas: replicas)
      end

      validate
      write_partitions_to_zookeeper
      wait_for_partitions
      cluster.reset_metadata
    end

    def watch_partitions(cv: nil, timeout: nil, &block)
      Kazoo.wait_for_watch(cv: cv, timeout: timeout) do |cb|
        partitions_result = cluster.zk.get(path: "/brokers/topics/#{name}", watcher: cb)
        raise Kazoo::Error, "Failed to retrieve partitions. Result code: #{partitions_result.fetch(:rc)}" if partitions_result.fetch(:rc) != Zookeeper::Constants::ZOK

        set_partitions_from_json(partitions_result.fetch(:data))
        yield(self.partitions) if block_given?
      end
    ensure
      @partitions = nil
    end

    def save
      raise Kazoo::Error, "The topic #{name} already exists!" if exists?
      validate

      write_config_to_zookeeper
      write_partitions_to_zookeeper

      wait_for_partitions
      cluster.reset_metadata
    end

    def destroy
      t = Thread.current
      cb = Zookeeper::Callbacks::WatcherCallback.create do |event|
        case event.type
        when Zookeeper::Constants::ZOO_DELETED_EVENT
          t.run if t.status == 'sleep'
        else
          raise Kazoo::Error, "Unexpected Zookeeper event: #{event.type}"
        end
      end

      result = cluster.zk.stat(path: "/brokers/topics/#{name}", watcher: cb)
      case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        # continue
      when Zookeeper::Constants::ZNONODE
        raise Kazoo::TopicNotFound, "Topic #{name} does not exist!"
      else
        raise Kazoo::Error, "Failed to monitor topic"
      end


      result = cluster.zk.create(path: "/admin/delete_topics/#{name}")
      case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        Thread.stop unless cb.completed?
      when Zookeeper::Constants::ZNODEEXISTS
        raise Kazoo::Error, "The topic #{name} is already marked for deletion!"
      else
        raise Kazoo::Error, "Failed to delete topic #{name}. Error code: #{result.fetch(:rc)}"
      end

      cluster.reset_metadata
    end

    def config
      @config ||= load_config_from_zookeeper
    end

    def config=(hash)
      return if hash.nil?
      @config = hash.inject({}) { |h, (k, v)| h[k.to_s] = v.to_s; h }
    end

    def set_config(key, value)
      config[key.to_s] = value.to_s
      write_config_to_zookeeper
    end

    def delete_config(key)
      config.delete(key.to_s)
      write_config_to_zookeeper
    end

    def reset_default_config
      @config = {}
      write_config_to_zookeeper
    end

    def set_partitions_from_json(json_payload)
      partition_json = JSON.parse(json_payload)
      raise Kazoo::VersionNotSupported if partition_json.fetch('version') != 1

      @partitions = partition_json.fetch('partitions').map do |(id, replicas)|
        partition(id.to_i, replicas: replicas.map { |b| cluster.brokers[b] })
      end

      @partitions.sort_by!(&:id)

      self
    end

    def set_config_from_json(json_payload)
      config_json = JSON.parse(json_payload)
      raise Kazoo::VersionNotSupported if config_json.fetch('version') != 1

      @config = config_json.fetch('config')

      self
    end

    def load_partitions_from_zookeeper
      result = cluster.zk.get(path: "/brokers/topics/#{name}")
      raise Kazoo::Error, "Failed to get list of partitions for #{name}. Result code: #{result.fetch(:rc)}" if result.fetch(:rc) != Zookeeper::Constants::ZOK
      set_partitions_from_json(result.fetch(:data)).partitions
    end

    def load_config_from_zookeeper
      result = cluster.zk.get(path: "/config/topics/#{name}")
      case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        # continue
      when Zookeeper::Constants::ZNONODE
        return {}
      else
        raise Kazoo::Error, "Failed to retrieve topic config"
      end

      set_config_from_json(result.fetch(:data)).config
    end

    def write_partitions_to_zookeeper
      path = "/brokers/topics/#{name}"
      data = JSON.generate(version: 1, partitions: partitions_as_json)

      result = cluster.zk.set(path: path, data: data)
      case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        # continue
      when Zookeeper::Constants::ZNONODE
        result = cluster.zk.create(path: path, data: data)
        raise Kazoo::Error, "Failed to write partitions to zookeeper. Result code: #{result.fetch(:rc)}" unless result.fetch(:rc) == Zookeeper::Constants::ZOK
      else
        raise Kazoo::Error, "Failed to write partitions to zookeeper. Result code: #{result.fetch(:rc)}"
      end

      cluster.reset_metadata
    end

    def write_config_to_zookeeper
      config_hash = config.inject({}) { |h, (k,v)| h[k.to_s] = v.to_s; h }
      config_json = JSON.generate(version: 1, config: config_hash)

      # Set topic config
      result = cluster.zk.set(path: "/config/topics/#{name}", data: config_json)
      case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        # continue
      when Zookeeper::Constants::ZNONODE
        result = cluster.zk.create(path: "/config/topics/#{name}", data: config_json)
        raise Kazoo::Error, "Failed to write topic config to zookeeper. Result code: #{result.fetch(:rc)}" unless result.fetch(:rc) == Zookeeper::Constants::ZOK
      else
        raise Kazoo::Error, "Failed to write topic config to zookeeper. Result code: #{result.fetch(:rc)}"
      end

      # Set config change notification
      result = cluster.zk.create(path: "/config/changes/config_change_", data: name.inspect, sequence: true)
      raise Kazoo::Error, "Failed to set topic config change notification" unless result.fetch(:rc) == Zookeeper::Constants::ZOK

      cluster.reset_metadata
    end

    def self.create(cluster, name, partitions: nil, replication_factor: nil, config: {})
      topic = new(cluster, name, config: config, partitions: [])
      raise Kazoo::Error, "Topic #{name} already exists" if topic.exists?

      replica_assigner = Kazoo::ReplicaAssigner.new(cluster)

      partitions.times do
        replicas = replica_assigner.assign(replication_factor)
        topic.append_partition(replicas: replicas)
      end

      topic.save
      topic
    end

    protected

    def wait_for_partitions
      threads = []
      partitions.each do |partition|
        threads << Thread.new do
          Thread.abort_on_exception = true
          partition.wait_for_leader
        end
      end
      threads.each(&:join)
    end

    def partitions_as_json
      partitions.inject({}) do |hash, partition|
        hash[partition.id] = partition.replicas.map(&:id)
        hash
      end
    end
  end
end
