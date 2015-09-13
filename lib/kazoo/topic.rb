module Kazoo
  class Topic
    VALID_TOPIC_NAMES = %r{\A[a-zA-Z0-9\\._\\-]+\z}
    BLACKLISTED_TOPIC_NAMES = %r{\A\.\.?\z}

    attr_reader :cluster, :name
    attr_writer :partitions

    def initialize(cluster, name)
      @cluster, @name = cluster, name
    end

    def self.from_json(cluster, name, json)
      raise Kazoo::VersionNotSupported unless json.fetch('version') == 1

      topic = new(cluster, name)
      topic.partitions = json.fetch('partitions').map do |(id, replicas)|
        topic.partition(id.to_i, replicas: replicas.map { |b| cluster.brokers[b] })
      end.sort_by(&:id)

      return topic
    end

    def partitions
      @partitions ||= begin
        result = cluster.zk.get(path: "/brokers/topics/#{name}")
        raise Kazoo::Error, "Failed to get list of partitions for #{name}. Result code: #{result.fetch(:rc)}" if result.fetch(:rc) != Zookeeper::Constants::ZOK

        partition_json = JSON.parse(result.fetch(:data))
        partition_json.fetch('partitions').map do |(id, replicas)|
          partition(id.to_i, replicas: replicas.map { |b| cluster.brokers[b] })
        end
      end
    end

    def partition(*args)
      Kazoo::Partition.new(self, *args)
    end

    def replication_factor
      partitions.map(&:replication_factor).min
    end

    def under_replicated?
      partitions.any?(:under_replicated?)
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

    def create
      raise Kazoo::Error, "The topic #{name} already exists!" if exists?
      validate

      result = cluster.zk.create(
        path: "/config/topics/#{name}",
        data: JSON.generate(version: 1, config: {})
      )

      if result.fetch(:rc) != Zookeeper::Constants::ZOK
        raise Kazoo::Error, "Failed to create topic config node for #{name}. Error code: #{result.fetch(:rc)}"
      end

      result = cluster.zk.create(
        path: "/brokers/topics/#{name}",
        data: JSON.generate(version: 1, partitions: partitions_as_json)
      )

      if result.fetch(:rc) != Zookeeper::Constants::ZOK
        raise Kazoo::Error, "Failed to create topic #{name}. Error code: #{result.fetch(:rc)}"
      end

      cluster.reset_metadata
      wait_for_partitions
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
      result = cluster.zk.get(path: "/config/topics/#{name}")
      case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        # continue
      when Zookeeper::Constants::ZNONODE
        return {}
      else
        raise Kazoo::Error, "Failed to retrieve topic config"
      end

      config = JSON.parse(result.fetch(:data))
      raise Kazoo::VersionNotSupported if config.fetch('version') != 1

      config.fetch('config')
    end

    def set_config(key, value)
      new_config = config
      new_config[key.to_s] = value.to_s
      write_config(new_config)
    end

    def delete_config(key)
      new_config = config
      new_config.delete(key.to_s)
      write_config(new_config)
    end

    def reset_default_config
      write_config({})
    end

    def write_config(config_hash)
      raise Kazoo::TopicNotFound, "Topic #{name.inspect} does not exist" unless exists?

      config = config_hash.inject({}) { |h, (k,v)| h[k.to_s] = v.to_s; h }
      config_json = JSON.generate(version: 1, config: config)

      # Set topic config
      result = cluster.zk.set(path: "/config/topics/#{name}", data: config_json)
      case result.fetch(:rc)
      when Zookeeper::Constants::ZOK
        # continue
      when Zookeeper::Constants::ZNONODE
        result = cluster.zk.create(path: "/config/topics/#{name}", data: config_json)
        raise Kazoo::Error, "Failed to set topic config" unless result.fetch(:rc) == Zookeeper::Constants::ZOK
      else
        raise Kazoo::Error, "Failed to set topic config"
      end

      # Set config change notification
      result = cluster.zk.create(path: "/config/changes/config_change_", data: name.inspect, sequence: true)
      raise Kazoo::Error, "Failed to set topic config change notification" unless result.fetch(:rc) == Zookeeper::Constants::ZOK

      cluster.reset_metadata
    end

    def self.create(cluster, name, partitions: nil, replication_factor: nil)
      topic = new(cluster, name)
      topic.send(:sequentially_assign_partitions, partitions, replication_factor)
      topic.create
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

    def sequentially_assign_partitions(partition_count, replication_factor, brokers: nil)
      brokers = cluster.brokers.values if brokers.nil?
      raise ArgumentError, "replication_factor should be smaller or equal to the number of brokers" if replication_factor > brokers.length

      # Sequentially assign replicas to brokers. There might be a better way.
      @partitions = 0.upto(partition_count - 1).map do |partition_index|
        replicas = 0.upto(replication_factor - 1).map do |replica_index|
          broker_index = (partition_index + replica_index) % brokers.length
          brokers[broker_index]
        end

        self.partition(partition_index, replicas: replicas)
      end
    end

    def partitions_as_json
      partitions.inject({}) do |hash, partition|
        hash[partition.id] = partition.replicas.map(&:id)
        hash
      end
    end
  end
end
