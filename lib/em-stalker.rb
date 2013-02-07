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

  def job(j, options = {}, &blk)
    options = {:ttr => 300}.merge(options)
    @@jobs ||= {}
    @@jobs[j] = {:ttr => options[:ttr], :handler => blk}
  end
  
  def work(client_keys, options = {}, jobs = nil)
    prepare(client_keys, jobs)
    @@clients.select { |k,v| client_keys.include? k }.each do |key,connections|
      connections.each do |connection|
        connection.each_job(options) do |job|
          job_handler = @@jobs[job.tube][:handler]
          raise(NoSuchJob, job.tube) unless job_handler
          start_time = Time.now.utc.to_f
          begin
            ttr = @@jobs[job.tube][:ttr]
            Timeout::timeout(ttr - 2) do
              job    = before_job_handler.call(job, key)
              result = job_handler.call(job.body, key)
              job_success_handler.call(job, result, key)
            end
          rescue Exception => e
            job_error_handler.call(e, job, key)
          ensure
            after_job_handler.call(job, start_time, key)
          end
        end
      end
    end
  end
  
  # quit all connections
  def quit
    connections(:all).each { |c| c.quit }
  end    
  
  # set the on_error callback to all connections
  def on_error(&blk)
    connections(:all).each { |c| c.on_error(&blk) }
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
    @@logger ||= nil
    @@logger
  end
  
  def logger=(logger)
    @@logger = logger
  end
  
  def new_client(opts = {})
    opts = {:host => 'localhost', :port => 11300}.merge(opts)
    raise "It should define a key for beanstalkd client" unless opts.key?(:key)
    keys = opts[:key]
    keys = [keys] unless keys.respond_to?(:each)
    connection = Connection.new(opts)
    connection.fiber!
    @@clients ||= {}
    keys.each do |key|
      @@clients[key] ||= []
      @@clients[key] << connection 
    end
    logger.debug "Creating em-stalker connection #{opts}" if logger
    connection
  end
  
  private
  def connections(keys)
    if keys == :all
      @@clients.values.flatten.uniq { |connection| [connection.host, connection.port] }
    else
      @@clients.select { |k,v| keys.include?(k) }.values.flatten.uniq { |connection| [connection.host, connection.port] }
    end
  end

  def prepare(client_keys, jobs = nil)
    raise NoJobsDefined unless defined?(@@jobs)
    jobs ||= @@jobs.keys
    jobs.each do |job|
      raise(NoSuchJob, job) unless @@jobs[job]
    end
    jobs.each { |job| connections(client_keys).each { |c| c.watch(job) } }
    connections(client_keys).each do |connection|
      connection.watched_tubes.each do |tube|
        connection.ignore(tube) unless jobs.include?(tube)
      end
    end
  end  
  
  def job_success_handler
    @@job_success_handler ||= Proc.new { |job| job }
  end
  
  def before_job_handler
    @@before_job_handler ||= Proc.new { |job| job }
  end
  
  def after_job_handler
    @@after_job_handler ||= Proc.new { |job| job }
  end
  
  def job_error_handler
    @@job_error_handler ||= Proc.new do |e,job| 
      job.bury(65536)
      puts "EMStalker : Error on job #{job.tube} / #{job.body.to_s}"[0..150]
      puts [e.message,*e.backtrace].join("\n")
    end
  end
  
end
