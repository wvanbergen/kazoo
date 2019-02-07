module Kazoo
  class Partition
    attr_reader :topic, :id, :replicas

    def initialize(topic, id, replicas: nil)
      @topic, @id, @replicas = topic, id, replicas
      @mutex = Mutex.new
    end

    def cluster
      topic.cluster
    end

    def replication_factor
      replicas.length
    end

    def preferred_leader
      @replicas.first
    end

    def leader
      @mutex.synchronize do
        refresh_state
        @leader
      end
    end

    def isr
      @mutex.synchronize do
        refresh_state
        @isr
      end
    end

    def under_replicated?
      isr.length < replication_factor
    end

    def validate
      raise Kazoo::ValidationError, "No replicas defined for #{topic.name}/#{id}" if replicas.length == 0
      raise Kazoo::ValidationError, "The replicas of #{topic.name}/#{id} should be assigned to different brokers" if replicas.length > replicas.uniq.length

      true
    end

    def valid?
      validate
    rescue Kazoo::ValidationError
      false
    end

    def inspect
      "#<Kazoo::Partition #{topic.name}/#{id}>"
    end

    def key
      "#{topic.name}/#{id}"
    end

    def eql?(other)
      other.kind_of?(Kazoo::Partition) && topic == other.topic && id == other.id
    end

    alias_method :==, :eql?

    def hash
      [topic, id].hash
    end

    def wait_for_leader
      current_leader = nil
      while current_leader.nil?
        current_leader = begin
          leader
        rescue Kazoo::Error
          nil
        end

        sleep(0.1) if current_leader.nil?
      end
    end

    def to_json(generator)
      generator.generate(topic: topic.name, partition: id)
    end

    protected

    def refresh_state
      state_result = cluster.zk.get(path: "/brokers/topics/#{topic.name}/partitions/#{id}/state")
      raise Kazoo::Error, "Failed to get partition state. Error code: #{state_result.fetch(:rc)}" unless state_result.fetch(:rc) == Zookeeper::Constants::ZOK

      set_state_from_json(state_result.fetch(:data))
    end

    def set_state_from_json(json_payload)
      json = JSON.parse(json_payload)
      raise Kazoo::VersionNotSupported unless json.fetch('version') == 1

      begin
        @leader = cluster.brokers.fetch(json.fetch('leader'))
      rescue KeyError
        raise Kazoo::Error, "Topic #{topic.name}:#{id} has unknown leader! Brokers: '#{cluster.brokers.keys.join(',')}'; 'Payload: '#{json_payload.inspect}'"
      end

      @isr = json.fetch('isr').map do |r|
        begin
          cluster.brokers.fetch(r)
        rescue KeyError
          raise Kazoo::Error, "Broker #{r} not in cluster but in ISR list #{json.fetch('isr').inspect}?!"
        end
      end
    end
  end
end
