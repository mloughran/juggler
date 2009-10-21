class Juggler
  class Runner
    class << self
      def start
        @started ||= begin
          Signal.trap('INT') { EM.stop }
          Signal.trap('TERM') { EM.stop }
          true
        end
      end
    end

    def initialize(method, concurrency, strategy)
      @strategy = strategy
      @concurrency = concurrency
      @queue = method.to_s
      @running = []
    end

    def reserve
      beanstalk_job = connection.reserve(0)

      begin
        params = Marshal.load(beanstalk_job.body)
      rescue => e
        handle_exception(e, "Exception unmarshaling job")
        beanstalk_job.delete
        return
      end

      begin
        job = @strategy.call(params)
      rescue => e
        handle_exception(e, "Exception calling strategy")
        beanstalk_job.decay
        return
      end

      @running << job
      job.callback do
        @running.delete(job)
        beanstalk_job.delete
      end
      job.errback do
        @running.delete(job)
        # Built in exponential backoff
        beanstalk_job.decay
      end
    rescue Beanstalk::TimedOut
    rescue Beanstalk::NotConnected
      Juggler.logger.fatal "Could not connect any beanstalk hosts. " \
        "Retrying in 1s."
      sleep 1
    end

    def run
      EM.add_periodic_timer do
        reserve if spare_slot?
      end
      Runner.start
    end

    private

    def spare_slot?
      @running.size < @concurrency
    end

    def handle_exception(e, message)
      Juggler.logger.error "#{message}: #{e.class} #{e.message}"
      Juggler.logger.debug e.backtrace.join("\n")
    end

    def connection
      @pool ||= begin
        pool = Beanstalk::Pool.new(Juggler.hosts)
        pool.watch(@queue)
        pool
      end
    end
  end
end
