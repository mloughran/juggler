require 'beanstalk-client'

autoload :Logger, 'logger'

class Juggler
  class << self
    attr_writer :hosts, :logger

    def hosts
      @hosts ||= ['localhost:11300']
    end

    def logger
      @logger ||= begin
        logger = Logger.new(STDOUT)
        logger.level = Logger::WARN
        logger.debug("Created logger")
        logger
      end
    end

    def throw(method, params, options = {})
      # TODO: Do some checking on the method
      connection.use(method.to_s)

      priority = options[:priority] || 50
      delay = 0
      # Add 2s because we want to handle the timeout before beanstalk does
      ttr = (options[:ttr] || 60) + 2

      connection.put(Marshal.dump(params), priority, delay, ttr)
    end

    def juggle(method, concurrency = 1, &strategy)
      Runner.new(method, concurrency, strategy).run
    end

    private

    def connection
      @connection ||= Beanstalk::Pool.new(hosts)
    end
  end
end

Juggler.autoload 'Runner', 'juggler/runner'
