module Kazoo
  class Broker
    attr_reader :cluster, :id, :host, :port, :jmx_port

    def initialize(cluster, id, host, port, jmx_port: nil)
      @cluster = cluster
      @id, @host, @port = id, host, port
      @jmx_port = jmx_port
    end

    def self.from_json(cluster, id, json)
      new(cluster, id.to_i, json.fetch('host'), json.fetch('port'), jmx_port: json.fetch('jmx_port', nil))
    end

    def led_partitions
      cluster.partitions.select do |partition|
        partition.leader == self
      end
    end

    def replicated_partitions
      cluster.partitions.select do |partition|
        partition.replicas.include?(self)
      end
    end

    def critical?(replicas: 1)
      result, threads, mutex = false, ThreadGroup.new, Mutex.new
      replicated_partitions.each do |partition|
        t = Thread.new do
          isr = partition.isr.reject { |r| r == self }
          mutex.synchronize { result = true if isr.length < replicas }
        end
        threads.add(t)
      end
      threads.list.each(&:join)
      result
    end

    def eql?(other)
      other.is_a?(Kazoo::Broker) && other.cluster == self.cluster && other.id == self.id
    end

    def hash
      [self.cluster, self.id].hash
    end

    alias_method :==, :eql?
  end
end
