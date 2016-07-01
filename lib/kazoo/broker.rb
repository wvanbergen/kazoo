module Kazoo

  # Kazoo::Broker represents a Kafka broker in a Kafka cluster.
  class Broker
    attr_reader :cluster, :id, :host, :port, :jmx_port

    def initialize(cluster, id, host, port, jmx_port: nil)
      @cluster = cluster
      @id, @host, @port = id, host, port
      @jmx_port = jmx_port
    end

    # Returns a list of all partitions that are currently led by this broker.
    def led_partitions
      result, mutex = [], Mutex.new
      threads = cluster.partitions.map do |partition|
        Thread.new do
          Thread.abort_on_exception = true
          select = partition.leader == self
          mutex.synchronize { result << partition } if select
        end
      end
      threads.each(&:join)
      result
    end

    # Returns a list of all partitions that host a replica on this broker.
    def replicated_partitions
      result, mutex = [], Mutex.new
      threads = cluster.partitions.map do |partition|
        Thread.new do
          Thread.abort_on_exception = true
          select = partition.replicas.include?(self)
          mutex.synchronize { result << partition } if select
        end
      end
      threads.each(&:join)
      result
    end

    # Returns whether this broker is currently considered critical.
    #
    # A broker is considered critical if it is the only in sync replica
    # of any of the partitions it hosts. This means that if this broker
    # were to go down, the partition woild become unavailable for writes,
    # and may also lose data depending on the configuration and settings.
    def critical?(replicas: 1)
      result, mutex = false, Mutex.new
      threads = replicated_partitions.map do |partition|
        Thread.new do
          Thread.abort_on_exception = true
          isr = partition.isr.reject { |r| r == self }
          mutex.synchronize { result = true if isr.length < Integer(replicas) }
        end
      end
      threads.each(&:join)
      result
    end

    # Returns the address of this broker, i.e. the hostname plus the port
    # to connect to.
    def addr
      "#{host}:#{port}"
    end

    def eql?(other)
      other.is_a?(Kazoo::Broker) && other.cluster == self.cluster && other.id == self.id
    end

    alias_method :==, :eql?

    def hash
      [self.cluster, self.id].hash
    end

    def inspect
      "#<Kazoo::Broker id=#{id} addr=#{addr}>"
    end

    # Instantiates a Kazoo::Broker instance based on the Broker metadata that is stored
    # in Zookeeper under `/brokers/<id>`.
    # TODO: add support for endpoints in Kafka 0.9+
    def self.from_json(cluster, id, json)
      case json.fetch('version')
      when 1, 2, 3
        new(cluster, id.to_i, json.fetch('host'), json.fetch('port'), jmx_port: json.fetch('jmx_port', nil))
      else
        raise Kazoo::VersionNotSupported
      end
    end
  end
end
