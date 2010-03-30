class Juggler
  class JobRunner
    attr_reader :job
    
    def initialize(job, params, strategy)
      @job = job
      @params = params
      @strategy = strategy
    end
    
    def run
      dd = EM::DefaultDeferrable.new
      Juggler.logger.info "Running #{@job.jobid}"
      
      @running = run_strategy
      @running.callback {
        delete
        dd.succeed
      }
      @running.errback { |e|
        release
        dd.fail e
      }
      
      dd
    end
    
    def check_for_timeout
      job.stats { |stats| 
        puts "Job #{job.jobid} has #{stats["time-left"]}s left"
        if stats["time-left"] < 1
          @running.fail "Timed out"
        end
      }
    end
    
    private
    
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
      delete_def = job.delete
      delete_def.callback do
        Juggler.logger.debug "Job #{job.jobid} deleted"
      end
      delete_def.errback do
        Juggler.logger.debug "Job #{job.jobid} delete operation failed"
      end
      delete_def
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
