# Special event machine state machine
# You can define callback on enter and on exit for methods
# Methods can be async, which means that they will block until
# 
# state :foobar, :enter => 'get_http'
# 
module Juggler::StateMachine
  def self.included(klass)
    klass.extend(ClassMethods)
  end
  
  def state
    @_state
  end
  
  def change_state(new_state)
    old_state = @_state
    
    Juggler.logger.debug "Changing state: #{old_state} to #{new_state}"
    
    return nil if old_state == new_state
    
    if method = self.class.states[new_state][:pre]
      deferable = self.send(method)
      deferable.callback {
        run_synchronous_callbacks(old_state, new_state)
      }
      deferable.errback {
        Juggler.logger.warn "State change aborted - pre failed"
      }
    else
      run_synchronous_callbacks(old_state, new_state)
    end

    return true
  end

  def bind(state, &callback)
    @on_state ||= Hash.new { |h, k| h[k] = [] }
    @on_state[state] << callback
  end

  private

  def run_synchronous_callbacks(old_state, new_state)
    catch :halt do
      if callbacks = self.class.states[old_state][:exit]
        [callbacks].flatten.each { |c| self.send(c) }
      end

      if callbacks = self.class.states[new_state][:enter]
        [callbacks].flatten.each { |c| self.send(c) }
      end
      
      if @on_state && (callbacks = @on_state[new_state])
        callbacks.each { |c| c.call(self) }
      end
    end
    
    @_state = new_state
  end
  
  module ClassMethods
    def states
      @_states
    end
  
    def state(name, callbacks = {})
      @_states ||= {}
      @_states[name] = callbacks
    end
  end
end
