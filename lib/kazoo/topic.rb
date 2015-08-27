module Kazoo
  class Topic
    VALID_TOPIC_NAMES = %r{\A[a-zA-Z0-9\\._\\-]+\z}
    BLACKLISTED_TOPIC_NAMES = %r{\A\.\.?\z}

    attr_reader :cluster, :name
    attr_accessor :partitions

    def initialize(cluster, name)
      @cluster, @name = cluster, name
      @partitions = []
    end

    def self.from_json(cluster, name, json)
      topic = new(cluster, name)
      topic.partitions = json.fetch('partitions').map do |(id, replicas)|
        topic.partition(id.to_i, replicas: replicas.map { |b| cluster.brokers[b] })
      end.sort_by(&:id)

      return topic
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
        path: "/brokers/topics/#{name}",
        data: JSON.dump(version: 1, partitions: partitions_as_json)
      )

      if result.fetch(:rc) != Zookeeper::Constants::ZOK
        raise Kazoo::Error, "Failed to create topic #{name}. Error code: #{result.fetch(:rc)}"
      end

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
        raise Kazoo::Error, "Topic #{name} does not exist!"
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
