require 'beanstalk-client'

autoload :Logger, 'logger'

class Juggler
  class << self
    attr_writer :logger

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
      connection.put(Marshal.dump(params))
    end

    def juggle(method, concurrency = 1, &strategy)
      Runner.new(method, concurrency, strategy).run
    end

    private

    def connection
      @connection ||= Beanstalk::Pool.new('localhost:11300')
    end
  end
end

Juggler.autoload 'Runner', 'juggler/runner'
