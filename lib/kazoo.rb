require 'zookeeper'
require 'json'
require 'thread'
require 'socket'
require 'securerandom'
require 'bigdecimal'
require 'time'

module Kazoo
  Error = Class.new(StandardError)

  ValidationError = Class.new(Kazoo::Error)
  VersionNotSupported = Class.new(Kazoo::Error)
  NoClusterRegistered = Class.new(Kazoo::Error)
  TopicNotFound = Class.new(Kazoo::Error)
  ConsumerInstanceRegistrationFailed = Class.new(Kazoo::Error)
  PartitionAlreadyClaimed = Class.new(Kazoo::Error)
  ReleasePartitionFailure = Class.new(Kazoo::Error)
  InvalidSubscription = Class.new(Kazoo::Error)
  InconsistentSubscriptions = Class.new(Kazoo::Error)
  NoRunningInstances = Class.new(Kazoo::Error)

  def self.connect(zookeeper)
    Kazoo::Cluster.new(zookeeper)
  end


  def self.wait_for_watch(cv: nil, timeout: nil, &block)
    m, cv, result = Mutex.new, cv || ConditionVariable.new, false
    cb = Zookeeper::Callbacks::WatcherCallback.create { result = true; cv.broadcast }

    m.synchronize do
      yield(cb)
      cv.wait(m, timeout) unless cb.completed?
    end

    return result
  end
end

require 'kazoo/cluster'
require 'kazoo/broker'
require 'kazoo/topic'
require 'kazoo/partition'
require 'kazoo/replica_assigner'
require 'kazoo/subscription'
require 'kazoo/consumergroup'
require 'kazoo/version'
