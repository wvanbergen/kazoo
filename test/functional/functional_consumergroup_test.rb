require 'test_helper'

class FunctionalConsumergroupTest < Minitest::Test
  def setup
    zookeeper = ENV["ZOOKEEPER_PEERS"] || "127.0.0.1:2181"
    @cluster = Kazoo.connect(zookeeper)
    @cg = Kazoo::Consumergroup.new(@cluster, 'test.kazoo')
    @cg.create
  end

  def teardown
    cg = Kazoo::Consumergroup.new(@cluster, 'test.kazoo')
    cg.destroy if cg.exists?

    @cluster.close
  end

  def test_create_and_destroy_consumergroup
    cg = Kazoo::Consumergroup.new(@cluster, 'test.kazoo.2')
    refute cg.exists?

    cg.create
    assert cg.exists?

    cg.destroy
    refute cg.exists?
  end

  def test_retrieve_and_commit_offsets
    topic = Kazoo::Topic.new(@cluster, 'test.1')
    partition = Kazoo::Partition.new(topic, 0)

    assert_nil @cg.retrieve_offset(partition)

    @cg.commit_offset(partition, 1234)

    assert_equal 1234 + 1, @cg.retrieve_offset(partition)

    @cg.reset_all_offsets
    assert_nil @cg.retrieve_offset(partition)
  end

  def test_retrieve_all_offsets
    topic1 = Kazoo::Topic.new(@cluster, 'test.1')
    partition10 = Kazoo::Partition.new(topic1, 0)

    topic4 = Kazoo::Topic.new(@cluster, 'test.4')
    partition40 = Kazoo::Partition.new(topic4, 0)
    partition41 = Kazoo::Partition.new(topic4, 1)
    partition42 = Kazoo::Partition.new(topic4, 2)
    partition43 = Kazoo::Partition.new(topic4, 3)

    assert_equal Hash.new, @cg.retrieve_all_offsets

    @cg.commit_offset(partition10, 10)
    @cg.commit_offset(partition40, 40)
    @cg.commit_offset(partition41, 41)
    @cg.commit_offset(partition42, 42)
    @cg.commit_offset(partition43, 43)

    offsets = @cg.retrieve_all_offsets

    assert_equal 5, offsets.length
    assert_equal 11, offsets[partition10]
    assert_equal 41, offsets[partition40]
    assert_equal 42, offsets[partition41]
    assert_equal 43, offsets[partition42]
    assert_equal 44, offsets[partition43]

    @cg.reset_all_offsets
    assert_equal Hash.new, @cg.retrieve_all_offsets
  end

  def test_watch_instances
    topic = Kazoo::Topic.new(@cluster, 'test.1')

    instance1 = @cg.instantiate
    instance1.register([topic])
    instance2 = @cg.instantiate
    instance2.register([topic])

    t = Thread.current
    instances, cb = @cg.watch_instances { t.run if t.status == 'sleep' }
    assert_equal Set[instance1, instance2], Set.new(instances)

    Thread.new { instance2.deregister }

    Thread.stop unless cb.completed?

    assert assert_equal Set[instance1], Set.new(@cg.instances)
    instance1.deregister
  end
end
