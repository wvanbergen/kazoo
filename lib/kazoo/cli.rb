require 'kazoo'
require 'thor'

require 'kazoo/cli/common'
require 'kazoo/cli/cluster'
require 'kazoo/cli/consumers'
require 'kazoo/cli/topics'

module Kazoo
  class CLI < Thor
    include Kazoo::CLI::Common

    desc "cluster SUBCOMMAND ...ARGS", "Inspect the Kafka cluster"
    subcommand "cluster", Kazoo::CLI::Cluster

    desc "topics SUBCOMMAND ...ARGS", "Manage consumer groups"
    subcommand "topics", Kazoo::CLI::Topics

    desc "consumers SUBCOMMAND ...ARGS", "Manage consumer groups"
    subcommand "consumers", Kazoo::CLI::Consumers
  end
end
