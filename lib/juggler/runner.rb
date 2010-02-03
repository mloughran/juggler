class Juggler
  # Stopping: This is rather complex. The point of the __STOP__ malarkey it to 
  # unblock a blocking reserve so that delete and release commands can be 
  # actioned on the currently running jobs before shutdown. Also a 
  # SHUTDOWN_GRACE period is availble for jobs to complete before the 
  # eventmachine is stopped
  # 
  class Runner
    SHUTDOWN_GRACE = 10
    
    class << self
      def start(runner)
        @runners ||= []
        @runners << runner
        
        @signals_setup ||= begin
          %w{INT TERM}.each do |sig|
            Signal.trap(sig) {
              stop_all_runners_with_grace
            }
          end
          true
        end
      end
      
      private
      
      def stop_all_runners_with_grace
        # Trigger each runner to shut down
        @runners.each { |r| r.stop }
        
        puts "Giving processes #{SHUTDOWN_GRACE}s grace period to exit"
        
        EM::PeriodicTimer.new(0.1) {
          if !@runners.any? { |r| r.running? }
            puts "Exited cleanly"
            EM.stop
          end
        }
        
        EM::Timer.new(SHUTDOWN_GRACE) do
          puts "Force exited after #{SHUTDOWN_GRACE}s with tasks running"
          EM.stop
        end
      end
    end

    def initialize(method, concurrency, strategy)
      @strategy = strategy
      @concurrency = concurrency
      @queue = method.to_s
      
      @running = []
      @reserved = false
    end
    
    # We potentially need to issue a new reserve call after a job is reserved 
    # (if we're not at the concurrency limit), and after a job completes 
    # (unless we're already reserving)
    def reserve_if_necessary
      if @on && !@reserved && @running.size < @concurrency
        reserve
      end
    end

    def reserve
      reserve_call = connection.reserve
      @reserved = true
      
      reserve_call.callback do |job|
        @reserved = false
        
        EM.next_tick {
          # Reserve in next tick so that on error deletes get scheduled first
          reserve_if_necessary
        }

        begin
          params = Marshal.load(job.body)
        rescue => e
          handle_exception(e, "Exception unmarshaling #{@queue} job")
          connection.delete(job)
          next
        end
        
        if params == "__STOP__"
          connection.delete(job)
          next
        end

        begin
          job_deferrable = @strategy.call(params)
        rescue => e
          handle_exception(e, "Exception calling #{@queue} strategy")
          
          # TODO: exponential backoff, error catching
          connection.release(job, :delay => 1)
          
          next
        end
        
        @running << job
        puts "DEBUG: Queue #{@queue}: Excecuting #{@running.size} jobs"

        job_deferrable.callback do
          @running.delete(job)
          
          # TODO: error catching
          connection.delete(job)

          reserve_if_necessary
        end

        job_deferrable.errback do |e|
          @running.delete(job)
          
          # TODO: exponential backoff, error catching
          connection.release(job, :delay => 1)

          reserve_if_necessary
        end
      end
      
      reserve_call.errback do |error|
        @reserved = false
        
        puts "Error from reserve call: #{error}"
        
        # Wait 1s before reserving or we'll just get DEALINE_SOON again
        # "If the client issues a reserve command during the safety margin, 
        # <snip>, the server will respond with: DEADLINE_SOON"
        EM::Timer.new(1) do
          reserve_if_necessary
        end
      end
    end

    def run
      @on = true
      Runner.start(self)
      reserve_if_necessary
    end

    def stop
      @on = false

      # See class documentation on stopping
      if @reserved
        Juggler.throw(@queue, "__STOP__")
      end
    end
    
    def running?
      @running.size > 0
    end

    private

    def handle_exception(e, message)
      Juggler.logger.error "#{message}: #{e.message} (#{e.class})"
      Juggler.logger.debug e.backtrace.join("\n")
    end

    def connection
      @connection ||= EMJack::Connection.new({
        :host => "localhost",
        :tube => @queue
      })
    end
  end
end
