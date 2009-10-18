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
      params = Marshal.load(beanstalk_job.body)
      job = @strategy.call(params)
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

    def connection
      @pool ||= begin
        pool = Beanstalk::Pool.new('localhost:11300')
        pool.watch(@queue)
        pool
      end
    end
  end
end
