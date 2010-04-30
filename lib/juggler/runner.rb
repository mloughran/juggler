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
        
        Juggler.logger.info {
          "Giving processes #{SHUTDOWN_GRACE}s grace period to exit"
        }
        
        EM::PeriodicTimer.new(0.1) {
          if !@runners.any? { |r| r.running? }
            Juggler.logger.info "Exited cleanly"
            EM.stop
          end
        }
        
        EM::Timer.new(SHUTDOWN_GRACE) do
          Juggler.logger.info {
            "Force exited after #{SHUTDOWN_GRACE}s with tasks running"
          }
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
        Juggler.logger.debug "#{to_s}: Reserving"
        reserve
      end
    end

    def reserve
      @reserved = true
      
      reserve_call = connection.reserve
      
      reserve_call.callback do |job|
        @reserved = false
        
        EM.next_tick {
          # Reserve in next tick so that any errors during this reserve can be 
          # excecuted before the next blocking reserve
          reserve_if_necessary
        }

        begin
          params = Marshal.load(job.body)
        rescue => e
          handle_exception(e, "#{to_s}: Exception unmarshaling #{@queue} job")
          connection.delete(job)
          next
        end
        
        if params == "__STOP__"
          connection.delete(job)
          next
        end
        
        job_runner = JobRunner.new(job, params, @strategy)
        
        @running << job_runner

        Juggler.logger.debug {
          "#{to_s}: Excecuting #{@running.size} jobs"
        }

        jdd = job_runner.run
        jdd.callback do
          @running.delete(job_runner)
          reserve_if_necessary
        end
        jdd.errback do |e|
          @running.delete(job_runner)
          reserve_if_necessary
        end
      end
      
      reserve_call.errback do |error|
        @reserved = false
        
        if error == :deadline_soon
          # This doesn't necessarily mean that a job has taken too long, it is 
          # quite likely that the blocking reserve is just stopping jobs from 
          # being deleted
          Juggler.logger.debug "#{to_s}: Reserve terminated (deadline_soon)"
          
          # TODO: Check job timeout only if deadline_soon
          check_all_reserved_jobs.callback {
            reserve_if_necessary
          }
        else
          Juggler.logger.error "#{to_s}: Unexpected error: #{error}"
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
    
    def to_s
      "Queue #{@queue}"
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
    
    # Iterates over all jobs reserved on this connection and fails them if 
    # they're within 1s of their timeout. Returns a callback which completes 
    # when all jobs have been checked
    def check_all_reserved_jobs
      dd = EM::DefaultDeferrable.new
      
      @running.each do |job_runner|
        job_runner.check_for_timeout
      end
      
      # TODO: do this properly

      # Wait 1s before reserving or we'll just get DEALINE_SOON again
      # "If the client issues a reserve command during the safety margin, 
      # <snip>, the server will respond with: DEADLINE_SOON"
      EM::Timer.new(1) do
        dd.succeed
      end
      
      dd
    end
  end
end
