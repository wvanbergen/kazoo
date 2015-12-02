module Kazoo

  # Helper class to reassign replicas to brokers.
  class ReplicaReassigner
    attr_reader :cluster

    def initialize(cluster)
      @cluster = cluster
    end

    def self.agony(from, to)
      (to - from).length
    end

    def self.valid_replicaset?(replicas, minimum_replicas: 1)
      replicas == replicas.uniq && replicas.length >= minimum_replicas
    end

    def self.safe_reassignment?(from, to, max_agony: 1, minimum_replicas: 1)
      !(from & to).empty? && valid_replicaset?(to, minimum_replicas: minimum_replicas) && agony(from, to) <= max_agony
    end

    def self.steps(from, to, max_agony: 1, minimum_replicas: nil, include_initial: false)
      minimum_replicas ||= [from.length, to.length].min
      missing  = to - from
      unneeded = from - to

      steps = include_initial ? [from.clone] : []

      current_step = from.clone
      until missing.empty? && unneeded.empty?
        add_missing   = missing.pop(max_agony)
        drop_unneeded = unneeded.pop([current_step.length - 1, max_agony].min)

        next_step = current_step - drop_unneeded + add_missing

        raise "Cannot generate safe reassignment plan" unless safe_reassignment?(current_step, next_step, max_agony: max_agony, minimum_replicas: minimum_replicas)

        steps << (current_step = next_step).clone
      end

      # This method provides the right steps, but does not necessarily end up with the replicas
      # in the right order. Because recordering the replicas is basically a no-op, we simply
      # replace the last step with the desired order.
      raise "Unexpectedly did not end up with the desired replicaset" unless Set.new(to) == Set.new(steps[steps.length - 1])
      steps[steps.length - 1] = to

      steps
    end
  end
end
