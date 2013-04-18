class Juggler
  class Runner
    @runners = []

    class << self
      def start(runner)
        @runners << runner
      end
      
      # Stop all runners (stops accepting new jobs)
      #
      # Returns a deferrable which succeeds when all jobs have finished or
      # fails if some jobs are still running after a timeout (default 2s,
      # configurable by setting Juggler.shutdown_grace_timeout)
      def stop
        df = EM::DefaultDeferrable.new

        # Trigger each runner to shut down
        @runners.each { |r| r.stop }
        
        grace_time = Juggler.shutdown_grace_timeout

        Juggler.logger.info {
          "Giving processes #{grace_time}s grace period to exit"
        }
        
        EM::PeriodicTimer.new(0.1) {
          df.succeed if !@runners.any? { |r| r.running? }
        }
        
        EM::Timer.new(grace_time) { df.fail }

        return df
      end

      # Avoid breaking compatability
      alias :stop_all_runners_with_grace :stop
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
      if @on && @connection.connected? && !@reserved && @running.size < @concurrency
        Juggler.logger.debug "#{to_s}: Reserving"
        reserve
      end
    end

    def reserve
      @reserved = true
      
      reserve_call = connection.reserve
      
      reserve_call.callback do |job|
        @reserved = false

        begin
          params = Juggler.serializer.load(job.body)
        rescue => e
          handle_exception(e, "#{to_s}: Exception unserializing #{@queue} job")
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

        # We may reserve after job is running (after fetching stats)
        job_runner.bind(:running) {
          reserve_if_necessary
        }

        # Also may reserve when a job is done
        job_runner.bind(:done) {
          @running.delete(job_runner)
          reserve_if_necessary
        }

        job_runner.run
      end
      
      reserve_call.errback do |error|
        @reserved = false
        
        if error == :deadline_soon
          # This doesn't necessarily mean that a job has taken too long, it is 
          # quite likely that the blocking reserve is just stopping jobs from 
          # being deleted
          Juggler.logger.debug "#{to_s}: Reserve terminated (deadline_soon)"

          check_all_reserved_jobs.callback {
            reserve_if_necessary
          }
        elsif error == :disconnected
          Juggler.logger.warn "#{to_s}: Reserve terminated (beanstalkd disconnected)"
        else
          Juggler.logger.error "#{to_s}: Unexpected error: #{error}"
          reserve_if_necessary
        end
      end
    end

    def run
      @on = true
      Runner.start(self)
      # Creates beanstalkd connection - reserve happens on connect
      connection
    end

    # Stopping a runner causes it to stop reserving any new jobs and to cancel
    # the current blocking reserve
    def stop
      @on = false

      # This is rather complex. The point of the __STOP__ malarkey it to
      # unblock a blocking reserve so that delete and release commands can be
      # actioned on the currently running jobs before shutdown.
      if @reserved
        Juggler.throw(@queue, "__STOP__")
      end
    end
    
    def running?
      @running.size > 0
    end
    
    # The number of jobs currently running.
    # This will be between 0 and @concurrency.
    def running_jobs
      @running.size
    end

    def to_s
      "Tube #{@queue}"
    end

    private

    def handle_exception(e, message)
      Juggler.logger.error "#{message}: #{e.message} (#{e.class})"
      Juggler.logger.debug e.backtrace.join("\n")
    end

    def connection
      @connection ||= begin
        c = EMJack::Connection.new({
          :host => Juggler.server.host,
          :port => Juggler.server.port,
        })
        c.on_connect {
          c.watch(@queue)
          reserve_if_necessary
        }
        c.on_disconnect {
          Juggler.send(:disconnected)
        }
        c
      end
    end
    
    # Iterates over all jobs reserved on this connection and fails them if 
    # they're within 1s of their timeout. Returns a callback which completes 
    # when all jobs have been checked
    def check_all_reserved_jobs
      dd = EM::DefaultDeferrable.new
      
      @running.each do |job_runner|
        job_runner.check_for_timeout
      end
      
      # Wait 1s before reserving or we'll just get DEALINE_SOON again
      # "If the client issues a reserve command during the safety margin, 
      # <snip>, the server will respond with: DEADLINE_SOON"
      #
      # In theory, one should not need to do this since reserve will already
      # be triggered as a callback on the job that has timed out
      EM::Timer.new(1) do
        dd.succeed
      end
      
      dd
    end
  end
end
