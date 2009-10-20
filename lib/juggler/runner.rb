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
    end

    def reserve
      reserve_call = connection.reserve
      
      reserve_call.callback do |job|
        params = Marshal.load(job.body)
        job_deferrable = @strategy.call(params)

        job_deferrable.callback do
          connection.delete(job)

          EM.next_tick(method(:reserve))
        end

        job_deferrable.errback do
          # TODO: exponential backoff
          connection.release(job, 1)

          EM.next_tick(method(:reserve))
        end
      end
      
      reserve_call.errback do |error|
        puts "Error from reserve call: #{error}"
        EM.add_timer(1, method(:reserve))
      end
    end

    def run
      Runner.start
      @concurrency.times { reserve }
    end

    private

    def connection
      @connection ||= EMJack::Connection.new({
        :host => "localhost",
        :tube => @queue
      })
    end
  end
end
