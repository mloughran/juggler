class Juggler
  class JobRunner
    attr_reader :job
    
    def initialize(job, params, strategy)
      @job = job
      @params = params
      @strategy = strategy
      Juggler.logger.debug {
        "#{to_s}: New job with body: #{params}"
      }
      @state = :pending
    end
    
    def run
      dd = EM::DefaultDeferrable.new
      Juggler.logger.info "#{to_s}: Running"
      
      @running = run_strategy
      @state = :running
      @running.callback {
        @state = :success
        delete.callback {
          dd.succeed
        }
      }
      @running.errback { |e|
        @state = :fail
        delete.release {
          dd.fail e
        }
      }
      
      dd
    end
    
    def check_for_timeout
      if @state == :running
        Juggler.logger.debug "#{to_s}: Fetching stats"
        job.stats { |stats| 
          Juggler.logger.debug "#{to_s}: #{stats["time-left"]}s left"
          if stats["time-left"] < 1
            @running.fail "Timed out"
          end
        }
      end
    end
    
    def to_s
      "Job #{@job.jobid}"
    end
    
    private
    
    # Wraps running the actual job.
    # Returns a deferrable that fails if there is an exception calling the 
    # strategy or if the strategy triggers errback
    def run_strategy
      dd = EM::DefaultDeferrable.new
      begin
        job_deferrable = @strategy.call(@params)
        job_deferrable.callback {
          dd.succeed
        }
        job_deferrable.errback {
          dd.fail
        }
        # Ugliness warning: on timeout dd will be failed externally
        dd.errback {
          job_deferrable.fail
        }
      rescue => e
        handle_exception(e, "Exception calling strategy")
        dd.fail
      end
      dd
    end
    
    # TODO: exponential backoff
    def release
      dd = EM::DefaultDeferrable.new
      
      Juggler.logger.debug { "Job #{job.jobid} releasing" }
      
      stats_def = job.stats
      stats_def.callback do |stats|
        Juggler.logger.debug { "Job #{job.jobid} stats: #{stats.inspect}"}
        
        release_def = job.release(:delay => 1)
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
    
    def delete
      dd = job.delete
      dd.callback do
        Juggler.logger.debug "Job #{job.jobid} deleted"
        dd.succeed
      end
      dd.errback do
        Juggler.logger.debug "Job #{job.jobid} delete operation failed"
        dd.succeed
      end
      dd
    end
    
    def handle_exception(e, message)
      Juggler.logger.error "#{message}: #{e.message} (#{e.class})"
      Juggler.logger.debug e.backtrace.join("\n")
    end
  end
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
