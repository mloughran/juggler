require 'em-jack'
require 'eventmachine'
require 'uri'

class Juggler
  class << self
    attr_writer :logger

    def server=(uri)
      @server = URI.parse(uri)
    end

    def server
      @server ||= URI.parse("beanstalk://localhost:11300")
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

Juggler.autoload 'Runner', 'juggler/runner'
Juggler.autoload 'JobRunner', 'juggler/job_runner'
