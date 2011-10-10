$:.unshift(File.dirname(__FILE__))

require 'timeout'

require 'em-stalker/job'
require 'em-stalker/errors'
require 'em-stalker/beanstalk_connection'
require 'em-stalker/connection'
require 'em-stalker/version'

Dir["#{File.dirname(__FILE__)}/em-stalker/handlers/*.rb"].each do |file|
  require file
end

module EMStalker
  extend self
  
  class NoJobsDefined < RuntimeError; end
  class NoSuchJob < RuntimeError; end
  
  def enqueue(tube, msg, opts = {})
    client.enqueue(tube, msg, opts)
  end

  def job(j, options = {}, &blk)
    options = {:ttr => 300}.merge(options)
    @@jobs ||= {}
    @@jobs[j] = {:ttr => options[:ttr], :handler => blk}
  end
  
  def work(options = {}, jobs = nil)
    prepare(jobs)
    client.each_job(options) do |job|
      job_handler = @@jobs[job.tube][:handler]
      raise(NoSuchJob, job.tube) unless job_handler
      begin
        ttr = @@jobs[job.tube][:ttr]
        Timeout::timeout(ttr - 2) do
          job = before_job_handler.call(job)
          job_handler.call(job.body)
          job_success_handler.call(job)
        end
      rescue Exception => e
        job_error_handler.call(e, job)
      ensure
        after_job_handler.call(job)
      end
    end
  end
  
  def quit
    client.quit
  end  
    
  def on_disconnect(&blk)
    client.on_disconnect(&blk)
  end
  
  def on_error(&blk)
    client.on_error(&blk)
  end
  
  def before_job(&blk)
    @@before_job_handler = blk 
  end
  
  def after_job(&blk)
    @@after_job_handler = blk 
  end
  
  def on_job_success(&blk)
    @@job_success_handler = blk 
  end
  
  def on_job_error(&blk)
    @@job_error_handler = blk    
  end
  
  def logger
    @@logger
  end
  
  def logger=(logger)
    @@logger = logger
  end
  
  def client
    unless defined?(@@client)
      @@client ||= Connection.new
      @@client.fiber!
    end
    @@client
  end
  
  private
  def prepare(jobs = nil)
    raise NoJobsDefined unless defined?(@@jobs)
    jobs ||= @@jobs.keys
    jobs.each do |job|
      raise(NoSuchJob, job) unless @@jobs[job]
    end
    jobs.each { |job| client.watch(job) }
    client.watched_tubes.each do |tube|
      client.ignore(tube) unless jobs.include?(tube)
    end
  end  
  
  def job_success_handler
    @@job_success_handler ||= Proc.new { |job| }
  end
  
  def before_job_handler
    @@before_job_handler ||= Proc.new { |job| }
  end
  
  def after_job_handler
    @@after_job_handler ||= Proc.new { |job| }
  end
  
  def job_error_handler
    @@job_error_handler ||= Proc.new do |e,job| 
      job.bury(65536)
      puts "EMStalker : Error on job #{job.tube} / #{job.body.to_s}"[0..150]
      puts [e.message,*e.backtrace].join("\n")
    end
  end
  
end