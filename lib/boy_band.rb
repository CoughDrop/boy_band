require 'json'
require 'resque'
require 'rails'

module BoyBand
  def self.job_instigator
    if defined?(PaperTrail)
      PaperTrail.whodunnit
    else
      'unknown'
    end
  end
  
  def self.set_job_instigator(name)
    if defined?(PaperTrail)
      PaperTrail.whodunnit = name
    end
  end
  
  module WorkerMethods
    def thread_id
      "#{Process.pid}_#{Thread.current.object_id}"
    end
  
    def schedule_for(queue, klass, method_name, *args)
      @queue = queue.to_s
      job_hash = Digest::MD5.hexdigest(args.to_json)
      note_job(job_hash)
      size = Resque.size(queue)
      if queue == :slow
        Resque.enqueue(SlowWorker, klass.to_s, method_name, *args)
        if size > 1000 && !Resque.redis.get("queue_warning_#{queue}")
          Resque.redis.setex("queue_warning_#{queue}", 5.minutes.to_i, "true")
          Rails.logger.error("job queue full: #{queue}, #{size} entries")
        end
      else
        Resque.enqueue(Worker, klass.to_s, method_name, *args)
        if size > 5000 && !Resque.redis.get("queue_warning_#{queue}")
          Resque.redis.setex("queue_warning_#{queue}", 5.minutes.to_i, "true")
          Rails.logger.error("job queue full: #{queue}, #{size} entries")
        end
      end
    end
  
    def note_job(hash)
      if Resque.redis
        timestamps = JSON.parse(Resque.redis.hget('hashed_jobs', hash) || "[]")
        cutoff = 6.hours.ago.to_i
        timestamps = timestamps.select{|ts| ts > cutoff }
        timestamps.push(Time.now.to_i)
  #      Resque.redis.hset('hashed_jobs', hash, timestamps.to_json)
      end
    end
  
    def clear_job(hash)
      if Resque.redis
        timestamps = JSON.parse(Resque.redis.hget('hashed_jobs', hash) || "[]")
        timestamps.shift
  #      Resque.redis.hset('hashed_jobs', hash, timestamps.to_json)
      end
    end
  
    def schedule(klass, method_name, *args)
      schedule_for(:default, klass, method_name, *args)
    end
  
    def perform(*args)
      perform_at(:normal, *args)
    end
  
    def ts
      Time.now.to_i
    end
    
    def in_worker_process?
      BoyBand.job_instigator.match(/^job/)
    end
  
    def perform_at(speed, *args)
      args_copy = [] + args
      klass_string = args_copy.shift
      klass = Object.const_get(klass_string)
      method_name = args_copy.shift
      job_hash = Digest::MD5.hexdigest(args_copy.to_json)
      hash = args_copy[0] if args_copy[0].is_a?(Hash)
      hash ||= {'method' => method_name}
      action = "#{klass_string} . #{hash['method']} (#{hash['id']})"
      pre_whodunnit = BoyBand.job_instigator
      BoyBand.set_job_instigator("job:#{action}")
      Rails.logger.info("performing #{action}")
      start = self.ts
      klass.last_scheduled_stamp = hash['scheduled'] if klass.respond_to?('last_scheduled_stamp=')
      klass.send(method_name, *args_copy)
      diff = self.ts - start
      Rails.logger.info("done performing #{action}, finished in #{diff}s")
      # TODO: way to track what queue a job is coming from
      if diff > 60 && speed == :normal
        Rails.logger.error("long-running job, #{action}, #{diff}s")
      elsif diff > 60*10 && speed == :slow
        Rails.logger.error("long-running job, #{action} (expected slow), #{diff}s")
      end
      BoyBand.set_job_instigator(pre_whodunnit)
      clear_job(job_hash)
    rescue Resque::TermException
      Resque.enqueue(self, *args)
    end
  
    def on_failure_retry(e, *args)
      # TODO...
    end
  
    def scheduled_actions(queue='default')
      queues = [queue]
      if queue == '*'
        queues = []
        Resque.queues.each do |key|
          queues << key
        end
      end

      res = []
      queues.each do |queue|
        idx = Resque.size(queue)
        idx.times do |i|
          res << Resque.peek(queue, i)
        end
      end
      res
    end
  
    def scheduled_for?(queue, klass, method_name, *args)
      idx = Resque.size(queue)
      queue_class = (queue == :slow ? 'SlowWorker' : 'Worker')
      if false
        job_hash = args.to_json
        timestamps = JSON.parse(Resque.redis.hget('hashed_jobs', job_hash) || "[]")
        cutoff = 6.hours.ago.to_i
        return timestamps.select{|ts| ts > cutoff }.length > 0
      else
        start = 0
        while start < idx
          items = Resque.peek(queue, start, 1000)
          start += items.length > 0 ? items.length : 1
          items.each do |schedule|
            if schedule && schedule['class'] == queue_class && schedule['args'][0] == klass.to_s && schedule['args'][1] == method_name.to_s
              a1 = args
              if a1.length == 1 && a1[0].is_a?(Hash)
                a1 = [a1[0].dup]
                a1[0].delete('scheduled')
              end
              a2 = schedule['args'][2..-1]
              if a2.length == 1 && a2[0].is_a?(Hash)
                a2 = [a2[0].dup]
                a2[0].delete('scheduled')
              end
              if a1.to_json == a2.to_json
                return true
              end
            end
          end
        end
      end
      return false
    end
  
    def scheduled?(klass, method_name, *args)
      scheduled_for?('default', klass, method_name, *args)
    end
  
    def stop_stuck_workers
      timeout = 8.hours.to_i
      Resque.workers.each {|w| w.unregister_worker if w.processing['run_at'] && Time.now - w.processing['run_at'].to_time > timeout}    
    end
  
    def prune_dead_workers
      Resque.workers.each{|w| w.prune_dead_workers }
    end
  
    def kill_all_workers
      Resque.workers.each{|w| w.unregister_worker }
    end
  
    def process_queues
      schedules = []
      Resque.queues.each do |key|
        while Resque.size(key) > 0
          schedules << {queue: key, action: Resque.pop(key)}
        end
      end
      schedules.each do |schedule|
        queue = schedule[:queue]
        schedule  = schedule[:action]
        if queue == 'slow'
          raise "unknown job: #{schedule.to_json}" if schedule['class'] != 'SlowWorker'
          SlowWorker.perform(*(schedule['args']))
        else
          raise "unknown job: #{schedule.to_json}" if schedule['class'] != 'Worker'
          Worker.perform(*(schedule['args']))
        end
      end
    end
  
    def queues_empty?
      found = false
      Resque.queues.each do |key|
        return false if Resque.size(key) > 0
      end
      true
    end
  
    def flush_queues
      if Resque.redis
        Resque.queues.each do |key|
          Resque.redis.ltrim("queue:#{key}", 1, 0)
        end
      end
      Resque.redis.del('hashed_jobs')
    end
  
    def transfer_backlog(queue)
      saves = []
      dos = []
      while Resque.size(queue) > 0 && (saves.length + dos.length) < 10000
        job = Resque.pop(queue)
        if job
          if job['args'] && job['args'][2] && job['args'][2]['method'] == 'track_downstream_boards!'
            saves.push(job)
          else
            dos.push(job)
          end
        end
      end
      dos.each{|job| Resque.enqueue(Worker, *job['args']) }; dos.length
      hash = saves.group_by{|j| j['args'][2]['id'] }; hash.length
      hash.each do |id, jobs|
        list = []
        jobs.each{|j| list += j['args'][2]['arguments'][0] }
        args = jobs[0]['args'][2]
        args['arguments'] = [list.uniq]
        Resque.enqueue(SlowWorker, job['args'][0], job['args'][1], args)
      end; hash.keys.length
      Resque.size(queue)
    end
  end
  
  module AsyncInstanceMethods
    def schedule(method, *args)
      return nil unless method
      id = self.id
      settings = {
        'id' => id,
        'method' => method,
        'scheduled' => self.class.scheduled_stamp,
        'arguments' => args
      }
      Worker.schedule(self.class, :perform_action, settings)
    end
  
    def schedule_once(method, *args)
      return nil unless method && id
      already_scheduled = Worker.scheduled?(self.class, :perform_action, {
        'id' => id,
        'method' => method,
        'scheduled' => self.class.scheduled_stamp,
        'arguments' => args
      })
      if !already_scheduled
        schedule(method, *args)
      else
        false
      end
    end

    def self.included(base)
      base.define_singleton_method(:included) do |klass|
        klass.cattr_accessor :last_scheduled_stamp
      end
    end
  end
  
  module AsyncClassMethods
    def scheduled_stamp
      Time.now.to_i
    end
  
    def schedule(method, *args)
      return nil unless method
      settings = {
        'method' => method,
        'scheduled' => self.scheduled_stamp,
        'arguments' => args
      }
      Worker.schedule(self, :perform_action, settings)
    end
  
    def schedule_once(method, *args)
      return nil unless method
      already_scheduled = Worker.scheduled?(self, :perform_action, {
        'method' => method,
        'scheduled' => self.scheduled_stamp,
        'arguments' => args
      })
      if !already_scheduled
        schedule(method, *args)
      else
        false
      end
    end
  
    def perform_action(settings)
      obj = self
      if settings['id']
        obj = obj.find_by(:id => settings['id'].to_s)
        obj.reload if obj
      end
      if !obj
        # record not found so there's nothing to do on it
        # TODO: probably log this somewhere so we don't lose it..
        Rails.logger.warn "expected record not found: #{self.to_s}:#{settings['id']}"
      elsif obj.respond_to?(settings['method'])
        obj.send(settings['method'], *settings['arguments'])
      else
        id = settings['id'] ? "#{settings['id']}:" : ""
        raise "method not found: #{self.to_s}:#{id}#{settings['method']}"
      end
    end
  end
end