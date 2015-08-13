require 'zookeeper'
require 'json'
require 'thread'

module Kazoo
  Error = Class.new(StandardError)

  NoClusterRegistered = Class.new(Kazoo::Error)
end

require 'kazoo/cluster'
require 'kazoo/broker'
require 'kazoo/topic'
require 'kazoo/partition'
require 'kazoo/version'
