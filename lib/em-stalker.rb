$:.unshift(File.dirname(__FILE__))

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
    jack.enqueue(tube, msg, opts)
  end
  
  def job(j, &blk)
    @@jobs ||= {}
    @@jobs[j] = blk
  end
  
  def work(jobs = nil)
    prepare(jobs)
    jack.each_job(0) do |job|
      job_handler = @@jobs[job.tube]
      raise(NoSuchJob, job.tube) unless job_handler
      begin
        job_handler.call(job.body)
        job.delete
      rescue SystemExit
        raise
      rescue => e
        job.bury rescue nil
        error_handler.call(e, job.tube, job.body)
      end
    end
  end
  
  def on_error(&blk)
    @@error_handler = blk    
  end
  
  private
  def prepare(jobs = nil)
    raise NoJobsDefined unless defined?(@@jobs)
    jobs ||= @@jobs.keys
    jobs.each do |job|
      raise(NoSuchJob, job) unless @@jobs[job]
    end
    jobs.each { |job| jack.watch(job) }
    jack.watched_tubes.each do |tube|
      jack.ignore(tube) unless jobs.include?(tube)
    end
  end
  
  def jack
    @@jack ||= EMJack::Connection.new
    @@jack
  end
  
  def error_handler
    @@error_handler || Proc.new { |e, tube, body| puts "Error on job #{tube} / #{body.to_s} #{e.backtrace}" }
  end
  
end