require 'juggler/state_machine'

class Juggler
  class JobRunner
    include StateMachine
    
    state :new
    state :running, :enter => :run_strategy
    state :succeeded, :enter => :delete
    state :timed_out, :enter => [:fail_strategy, :release]
    state :failed, :enter => :delete
    state :done
    
    attr_reader :job
    
    def initialize(job, params, strategy)
      @job = job
      @params = params
      @strategy = strategy
      Juggler.logger.debug {
        "#{to_s}: New job with body: #{params}"
      }
      @_state = :new
    end
    
    def run
      change_state(:running)
    end
    
    def check_for_timeout
      if state == :running
        Juggler.logger.debug "#{to_s}: Fetching stats"
        job.stats { |stats| 
          Juggler.logger.debug "#{to_s}: #{stats["time-left"]}s left"
          if stats["time-left"] < 1
            change_state(:timed_out)
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
      begin
        sd = @strategy.call(@params)
        sd.callback {
          change_state(:succeeded)
        }
        sd.errback { |e|
          # timed_out error is already handled
          change_state(:failed) unless e == :timed_out
        }
        @strategy_deferrable = sd
      rescue => e
        handle_exception(e, "Exception calling strategy")
        change_state(:failed)
      end
    end
    
    def fail_strategy
      @strategy_deferrable.fail(:timed_out)
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
          change_state(:done)
        }
        release_def.errback {
          Juggler.logger.error do
            "Job #{job.jobid } release failed (could not release)"
          end
          change_state(:done)
        }
      end
      stats_def.errback {
        Juggler.logger.error do
          "Job #{job.jobid } release failed (could not retrieve stats)"
        end
        change_state(:done)
      }
    end
    
    def delete
      dd = job.delete
      dd.callback do
        Juggler.logger.debug "Job #{job.jobid} deleted"
        change_state(:done)
      end
      dd.errback do
        Juggler.logger.debug "Job #{job.jobid} delete operation failed"
        change_state(:done)
      end
    end
    
    def handle_exception(e, message)
      Juggler.logger.error "#{message}: #{e.message} (#{e.class})"
      Juggler.logger.debug e.backtrace.join("\n")
    end
  end
end
