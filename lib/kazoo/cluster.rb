module Kazoo
  class Cluster

    attr_reader :zookeeper

    def initialize(zookeeper)
      @zookeeper = zookeeper
      @zk_mutex, @brokers_mutex, @topics_mutex = Mutex.new, Mutex.new, Mutex.new
    end

    def zk
      @zk_mutex.synchronize do
        @zk ||= Zookeeper.new(zookeeper)
      end
    end

    def brokers
      @brokers_mutex.synchronize do
        @brokers ||= begin
          brokers = zk.get_children(path: "/brokers/ids")

          if brokers.fetch(:rc) != Zookeeper::Constants::ZOK
            raise NoClusterRegistered, "No Kafka cluster registered on this Zookeeper location."
          end

          result, threads, mutex = {}, ThreadGroup.new, Mutex.new
          brokers.fetch(:children).map do |id|
            t = Thread.new do
              broker_info = zk.get(path: "/brokers/ids/#{id}")
              raise Kazoo::Error, "Failed to retrieve broker info. Error code: #{broker_info.fetch(:rc)}" unless broker_info.fetch(:rc) == Zookeeper::Constants::ZOK

              broker = Kazoo::Broker.from_json(self, id, JSON.parse(broker_info.fetch(:data)))
              mutex.synchronize { result[id.to_i] = broker }
            end
            threads.add(t)
          end
          threads.list.each(&:join)
          result
        end
      end
    end

    def consumergroups
      @consumergroups ||= begin
        consumers = zk.get_children(path: "/consumers")
        consumers.fetch(:children).map { |name| Kazoo::Consumergroup.new(self, name) }
      end
    end

    def topics
      @topics_mutex.synchronize do
        @topics ||= begin
          topics = zk.get_children(path: "/brokers/topics")
          raise Kazoo::Error, "Failed to list topics. Error code: #{topics.fetch(:rc)}" unless topics.fetch(:rc) == Zookeeper::Constants::ZOK

          result, threads, mutex = {}, ThreadGroup.new, Mutex.new
          topics.fetch(:children).each do |name|
            t = Thread.new do
              topic_info = zk.get(path: "/brokers/topics/#{name}")
              raise Kazoo::Error, "Failed to get topic info. Error code: #{topic_info.fetch(:rc)}" unless topic_info.fetch(:rc) == Zookeeper::Constants::ZOK

              topic = Kazoo::Topic.from_json(self, name, JSON.parse(topic_info.fetch(:data)))
              mutex.synchronize { result[name] = topic }
            end
            threads.add(t)
          end
          threads.list.each(&:join)
          result
        end
      end
    end

    def partitions
      topics.values.flat_map(&:partitions)
    end

    def reset_metadata
      @topics, @brokers = nil, nil
    end

    def under_replicated?
      partitions.any?(&:under_replicated?)
    end

    def close
      zk.close
    end
  end
end
