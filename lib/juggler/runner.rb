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
        reserve
      end
    end

    def reserve
      reserve_call = connection.reserve
      @reserved = true
      
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
          handle_exception(e, "Exception unmarshaling #{@queue} job")
          connection.delete(job)
          next
        end
        
        if params == "__STOP__"
          connection.delete(job)
          next
        end
        
        Juggler.logger.debug {
          "Job #{job.jobid} body: #{params}"
        }

        begin
          job_deferrable = @strategy.call(params)
        rescue => e
          handle_exception(e, "Exception calling #{@queue} strategy")
          release_for_retry(job)
          next
        end
        
        # job.stats do |stats|
        #   Juggler.logger.debug { "Job #{job.jobid} stats: #{stats.inspect}" }
        #   
        #   EM::Timer.new(stats["ttr"] - 2) {
        #     Juggler.logger.debug {
        #       "Job timeout exceeded - failing"
        #     }
        #     job_deferrable.fail "Timeout"
        #   }
        # end
        
        @running << job_deferrable
        Juggler.logger.debug {
          "Queue #{@queue}: Excecuting #{@running.size} jobs"
        }
        Juggler.logger.debug {
          @running.map { |e| e.inspect }.join("\n")
        }

        job_deferrable.callback do
          @running.delete(job_deferrable)
          
          delete_job(job).callback {
            reserve_if_necessary
          }
        end

        job_deferrable.errback do |e|
          @running.delete(job_deferrable)

          release_for_retry(job).callback {
            reserve_if_necessary
          }
        end
      end
      
      reserve_call.errback do |error|
        @reserved = false
        
        Juggler.logger.warn "Reserve call failed: #{error}"
        
        check_all_reserved_jobs
        
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
    
    # TODO: exponential backoff
    def release_for_retry(job)
      dd = EM::DefaultDeferrable.new
      Juggler.logger.debug { "Job #{job.jobid} releasing" }
      stats_def = job.stats
      stats_def.callback do |stats|
        Juggler.logger.debug { "Job #{job.jobid} stats: #{stats.inspect}"}
        
        release_def = connection.release(job, :delay => 1)
        
        release_def.callback {
          Juggler.logger.info { "Job #{job.jobid} released for retry" }
          dd.succeed
        }
        release_def.errback {
          Juggler.logger.error do
            "Job #{job.jobid } release failed (could not release)"
          end
          dd.succeed
        }
      end
      stats_def.errback {
        Juggler.logger.error do
          "Job #{job.jobid } release failed (could not retrieve stats)"
        end
        dd.succeed
      }
      dd
    end
    
    def delete_job(job)
      delete_def = connection.delete(job)
      delete_def.callback do
        Juggler.logger.debug "Job #{job.jobid} deleted"
      end
      delete_def.errback do
        Juggler.logger.debug "Job #{job.jobid} delete operation failed"
      end
      delete_def
    end
    
    def check_all_reserved_jobs
      @running.each do |job|
        puts "Checking job #{job.jobid}"
        job.stats { |stats|
          puts "Job #{job.jobid} has #{stats["time-left"]}s left"
          if stats["time-left"] < 1
            job.fail "Timed out"
          end
        }
      end
    end
  end
end
