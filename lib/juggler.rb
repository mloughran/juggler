require 'em-jack'
require 'eventmachine'
require 'uri'

class Juggler
  class << self
    attr_writer :logger
    attr_writer :shutdown_grace_timeout
    attr_accessor :exception_handler

    def server=(uri)
      @server = URI.parse(uri)
    end

    def server
      @server ||= URI.parse("beanstalk://localhost:11300")
    end

    # By default after receiving QUIT juggler will wait up to 2s for running
    # jobs to complete before killing them
    def shutdown_grace_timeout
      @shutdown_grace_timeout || 2
    end

    def logger
      @logger ||= begin
        require 'logger'
        logger = Logger.new(STDOUT)
        logger.level = Logger::WARN
        logger.debug("Created logger")
        logger
      end
    end

    def throw(method, params, options = {})
      # TODO: Do some checking on the method
      connection.use(method.to_s)
      connection.put(Marshal.dump(params), options)
    end

    # Strategy block: should return a deferrable object (so that juggler can 
    # apply callbacks and errbacks). You should note that this deferrable may 
    # be failed by juggler if the job timeout is exceeded, and therefore you 
    # are responsible for cleaning up your state (for example cancelling any 
    # timers which you have created)
    def juggle(method, concurrency = 1, &strategy)
      Runner.new(method, concurrency, strategy).run
    end

    private

    def connection
      @connection ||= EMJack::Connection.new({
        :host => server.host,
        :port => server.port
      })
    end
  end
end

# Default exception handler
Juggler.exception_handler = Proc.new do |e|
  Juggler.logger.error "Error running job: #{e.message} (#{e.class})"
  Juggler.logger.debug e.backtrace.join("\n")
end

Juggler.autoload 'Runner', 'juggler/runner'
Juggler.autoload 'JobRunner', 'juggler/job_runner'
