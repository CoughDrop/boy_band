require 'spec_helper'
require 'boy_band'
require 'active_support'
require 'fakeredis'
require 'rails'

describe BoyBand do
  module Async
    extend ActiveSupport::Concern
    include BoyBand::AsyncInstanceMethods
    
    module ClassMethods
      include BoyBand::AsyncClassMethods
    end
  end
  module Worker
    extend BoyBand::WorkerMethods

    def self.find_record(klass, id)
      klass.find_by(id: id)
    end
  end
  
  class AsyncObject
    include Async
    def id
      12345
    end
    
    def updated_at
      Time.parse("Jun 1, 2016")
    end
    
    def reload
      self
    end
    
    def self.count
      1
    end
    
    def self.find_by(*args)
      opts = JSON.parse(args[0].to_json)
      if opts['id'].to_s != '12345'
        return nil
      end
      AsyncObject.new
    end
  end
  
  class AsyncObject2 < AsyncObject
  end
  
  class FakeLogger
    def warn(*args)
    end
    
    def info(*args)
    end
  end
  
  before(:each) do
    Worker.flush_queues
    Worker.set_domain_id(nil)
    Resque.redis = Redis.new
    @logger = FakeLogger.new
    allow(Rails).to receive(:logger).and_return(@logger)
  end

  describe "async" do
    describe "object scheduling" do
      it "should not error on empty values" do
        u = AsyncObject.new
        expect(u.schedule(nil)).to eq(nil)
        expect(Worker.scheduled?(AsyncObject, :perform_action, {'id' => u.id, 'method' => nil, 'arguments' => []})).to be_falsey
      end
    
      it "should schedule events" do
        u = AsyncObject.new
        u.schedule(:do_something, 1, 2, 3)
        expect(Worker.scheduled?(AsyncObject, :perform_action, {'id' => u.id, 'method' => 'do_something', 'arguments' => [1,2,3]})).to be_truthy
      end
    end
  
    describe "class scheduling" do
      it "should not error on empty values" do
        expect(AsyncObject.schedule(nil)).to eq(nil)
        expect(Worker.scheduled?(AsyncObject, :perform_action, {'method' => nil, 'arguments' => []})).to be_falsey
      end
    
      it "should schedule events" do
        AsyncObject.schedule(:do_something, 1, 2, 3)
        expect(Worker.scheduled?(AsyncObject, :perform_action, {'method' => 'do_something', 'arguments' => [1,2,3]})).to be_truthy
      end
    end
  
    describe "perform_action" do
      it "should raise on invalid method" do
        expect{ AsyncObject.perform_action({'method' => 'bad_method', 'arguments' => []}) }.to raise_error("method not found: AsyncObject:bad_method")
      end
    
      it "should call class methods" do
        expect(AsyncObject).to receive(:hippo).with(1,2,3).and_return("ok")
        expect(AsyncObject.perform_action({'method' => 'hippo', 'arguments' => [1,2,3]})).to eq("ok")
      end
    
      it "should raise when the record isn't found" do
        expect(@logger).to receive(:warn).with("expected record not found: AsyncObject:5")
        AsyncObject.perform_action({'id' => 5, 'method' => 'id', 'arguments' => []})
      end
    
      it "should call object methods" do
        u = AsyncObject.new
        expect_any_instance_of(AsyncObject).to receive(:hippo).with(1,2,3).and_return("ok")
        expect(AsyncObject.perform_action({'id' => u.id, 'method' => 'hippo', 'arguments' => [1,2,3]})).to eq("ok")
      end
    end
  end
  
  describe "worker" do
    it "should properly flush queues" do
      Worker.schedule(AsyncObject, 'do_something', 2)
      expect(Worker.scheduled?(AsyncObject, :do_something, 2)).to eq(true)
      Worker.flush_queues
      expect(Worker.scheduled?(AsyncObject, :do_something, 2)).to eq(false)
    end
  
    describe "perform" do
      it "should parse out Worker options and call the appropriate method" do
        expect(AsyncObject).to receive(:bacon).with(12)
        Worker.perform('AsyncObject', 'bacon', 12)
      
        expect(AsyncObject2).to receive(:halo).with(6, {a: 1})
        Worker.perform('AsyncObject2', :halo, 6, {a: 1})
      end
    
      it "should run scheduled events when told" do
        Worker.schedule(AsyncObject, :bacon, 12)
        Worker.schedule(AsyncObject2, :halo, 6, {a: 1})
        expect(AsyncObject).to receive(:bacon).with(12)
        expect(AsyncObject2).to receive(:halo).with(6, {'a' => 1})
        Worker.process_queues
      end
    
      it "should catch termination exceptions and re-queue" do
        expect(AsyncObject).to receive(:bacon).with(12).and_raise(Resque::TermException.new('SIGTERM'))
        Worker.schedule(AsyncObject, :bacon, 12)
        Worker.process_queues
        expect(Worker.scheduled?(AsyncObject, :bacon, 12)).to be_truthy
      end
    end
  
    describe "schedule" do
      it "should add to the queue" do
        Worker.schedule(AsyncObject, 'do_something', 2)
        expect(Worker.scheduled?(AsyncObject, :do_something, 2)).to be_truthy
        expect(Worker.scheduled?(AsyncObject, :do_something, 1)).to be_falsey
        Worker.schedule(AsyncObject, 'do_something', {a: 1, b: [2,3,4], c: {d: 7}})
        expect(Worker.scheduled?(AsyncObject, :do_something, {a: 1, b: [2,3,4], c: {d: 7}})).to be_truthy
      end

      it "should add to a difference queue" do
        Worker.schedule_for('bacon', AsyncObject, 'do_something', 2)
        expect(Worker.scheduled?(AsyncObject, :do_something, 2)).to be_falsey
        expect(Worker.scheduled?(AsyncObject, :do_something, 1)).to be_falsey
        expect(Worker.scheduled_for?('bacon', AsyncObject, :do_something, 2)).to be_truthy
        expect(Worker.scheduled_for?('bacon', AsyncObject, :do_something, 1)).to be_falsey
        Worker.schedule_for('priority', AsyncObject, 'do_something', {a: 1, b: [2,3,4], c: {d: 7}})
        expect(Worker.scheduled?(AsyncObject, :do_something, {a: 1, b: [2,3,4], c: {d: 7}})).to be_falsey
        expect(Worker.scheduled_for?('priority', AsyncObject, :do_something, {a: 1, b: [2,3,4], c: {d: 7}})).to be_truthy
      end
    
      it "should add to the queue from async-enabled models" do
        AsyncObject.schedule(:hip_hop, 16)
        u = AsyncObject.new
        u.schedule(:hip_hop, 17)
        expect(Worker.scheduled?(AsyncObject, :perform_action, {'method' => 'hip_hop', 'arguments' => [16]})).to be_truthy
        expect(Worker.scheduled?(AsyncObject, :perform_action, {'id' => u.id, 'method' => 'hip_hop', 'arguments' => [17]})).to be_truthy
      end

      it "should attach the domain_id" do
        AsyncObject.schedule(:hip_hop, 16)
        expect(Worker.scheduled_actions[0]['domain_id']).to eq('default')
      end

      it "should use the updated domain_id" do
        Worker.set_domain_id('bacon.com')
        AsyncObject.schedule(:hip_hop, 16)
        expect(Worker.scheduled_actions[0]['domain_id']).to eq('bacon.com')
      end

      it "should allow classes to schedule_once" do
        expect(Worker.scheduled_actions.length).to eq(0)
        AsyncObject.schedule_once(:hip_hop, 17)
        AsyncObject.schedule_once(:hip_hop, 17)
        AsyncObject.schedule_once(:hip_hop, 17)
        expect(Worker.scheduled_actions.length).to eq(1)
      end

      it "should allow classes to schedule_for" do
        expect(Worker.scheduled_actions.length).to eq(0)
        expect(Worker.scheduled_actions('bacon').length).to eq(0)
        AsyncObject.schedule_for('bacon', :hip_hop, 17)
        expect(Worker.scheduled_actions.length).to eq(0)
        expect(Worker.scheduled_actions('bacon').length).to eq(1)
      end

      it "should allow classes to schedule_once_for" do
        expect(Worker.scheduled_actions.length).to eq(0)
        expect(Worker.scheduled_actions('bacon').length).to eq(0)
        AsyncObject.schedule_once_for('bacon', :hip_hop, 17)
        AsyncObject.schedule_once_for('bacon', :hip_hop, 17)
        AsyncObject.schedule_once_for('bacon', :hip_hop, 17)
        AsyncObject.schedule_once_for('bacon', :hip_hop, 17)
        AsyncObject.schedule_once_for('bacon', :hip_hop, 17)
        expect(Worker.scheduled_actions.length).to eq(0)
        expect(Worker.scheduled_actions('bacon').length).to eq(1)
      end

      it "should allow instances to schedule_once" do
        expect(Worker.scheduled_actions.length).to eq(0)
        u = AsyncObject.new
        u.schedule_once(:hip_hop, 17)
        u.schedule_once(:hip_hop, 17)
        u.schedule_once(:hip_hop, 17)
        u.schedule_once(:hip_hop, 17)
        u.schedule_once(:hip_hop, 17)
        expect(Worker.scheduled_actions.length).to eq(1)
      end

      it "should allow instances to schedule_for" do
        expect(Worker.scheduled_actions.length).to eq(0)
        expect(Worker.scheduled_actions('bacon').length).to eq(0)
        u = AsyncObject.new
        u.schedule_for('bacon', :hip_hop, 17)
        expect(Worker.scheduled_actions.length).to eq(0)
        expect(Worker.scheduled_actions('bacon').length).to eq(1)
      end

      it "should allow instances to schedule_once_for" do
        expect(Worker.scheduled_actions.length).to eq(0)
        expect(Worker.scheduled_actions('bacon').length).to eq(0)
        u = AsyncObject.new
        u.schedule_once_for('bacon', :hip_hop, 17)
        u.schedule_once_for('bacon', :hip_hop, 17)
        u.schedule_once_for('bacon', :hip_hop, 17)
        u.schedule_once_for('bacon', :hip_hop, 17)
        u.schedule_once_for('bacon', :hip_hop, 17)
        expect(Worker.scheduled_actions.length).to eq(0)
        expect(Worker.scheduled_actions('bacon').length).to eq(1)
      end

    end
  
    describe "perform_at" do
      it "should not log on short jobs" do
        expect(Worker).to receive(:ts).and_return(1469141072, 1469141072 + 10)
        expect(@logger).to_not receive(:error)
        Worker.perform_at(:normal, 'AsyncObject', 'count')
      end

      it "should log on long-running jobs" do
        expect(Worker).to receive(:ts).and_return(1469141072, 1469141072 + 65)
        expect(@logger).to receive(:error).with("long-running job, AsyncObject . count (), 65s")
        Worker.perform_at(:normal, 'AsyncObject', 'count')
      end
    
      it "should not log on semi-long jobs for the slow queue" do
        expect(Worker).to receive(:ts).and_return(1469141072, 1469141072 + (60*2))
        expect(@logger).to_not receive(:error)
        Worker.perform_at(:slow, 'AsyncObject', 'count')
      end
    
      it "should log on really-long jobs for the slow queue" do
        expect(Worker).to receive(:ts).and_return(1469141072, 1469141072 + (60*11))
        expect(@logger).to receive(:error).with("long-running job, AsyncObject . count () (expected slow), 660s")
        Worker.perform_at(:slow, 'AsyncObject', 'count')
      end
    end
  
    describe "scheduled_actions" do
      it "should have list actions" do
        Worker.schedule(AsyncObject, :something)
        expect(Worker.scheduled_actions.length).to eq(1)
        expect(Worker.scheduled_actions[-1]).to eq({
          'class' => 'Worker', 'args' => ['AsyncObject', 'something'], 'domain_id' => 'default'
        })
        u = AsyncObject.new
        u.schedule(:do_something, 'cool')
        expect(Worker.scheduled_actions.length).to be >= 2
        expect(Worker.scheduled_actions[-1]).to eq({
          'class' => 'Worker', 'args' => ['AsyncObject', 'perform_action', {'id' => u.id, 'method' => 'do_something', 'scheduled' => Time.now.to_i, 'arguments' => ['cool']}], 'domain_id' => 'default'
        })
      end
    end

    describe "stop_stuck_workers" do
      it "should have unregister only stuck workers" do
        worker1 = OpenStruct.new({
          :processing => {
            'run_at' => 6.weeks.ago
          }
        })
        worker2 = OpenStruct.new({
          :processing => {
            'run_at' => 1.seconds.ago
          }
        })
        worker3 = OpenStruct.new({
          :processing => {
          }
        })
        expect(Resque).to receive(:workers).and_return([worker1, worker2, worker3])
        expect(worker1).to receive(:unregister_worker)
        expect(worker2).to_not receive(:unregister_worker)
        expect(worker3).to_not receive(:unregister_worker)
        Worker.stop_stuck_workers
      end
    end

    describe "prune_dead_workers" do
      it "should prune dead workers" do
        worker1 = OpenStruct.new
        worker2 = OpenStruct.new
        worker3 = OpenStruct.new
        expect(Resque).to receive(:workers).and_return([worker1, worker2])
        expect(worker1).to receive(:prune_dead_workers)
        expect(worker2).to receive(:prune_dead_workers)
        expect(worker3).to_not receive(:prune_dead_workers)
        Worker.prune_dead_workers
      end
    end

    describe "kill_all_workers" do
      it "should kill all workers" do
        worker1 = OpenStruct.new
        worker2 = OpenStruct.new
        worker3 = OpenStruct.new
        expect(Resque).to receive(:workers).and_return([worker1, worker2])
        expect(worker1).to receive(:unregister_worker)
        expect(worker2).to receive(:unregister_worker)
        expect(worker3).to_not receive(:unregister_worker)
        Worker.kill_all_workers
      end
    end
  end  
end
