require File.expand_path('../../spec_helper', __FILE__)

# This spec describes the behaviour of job runner by mocking the interface to 
# beanstalk as exposed by em-jack
# 
describe Juggler::JobRunner do
  include EM::SpecHelper
  
  it "should run job and delete in the success case" do
    em(1) {
      job = mock(:job, {
        :jobid => 1, 
        :stats => stub_deferrable({"time-left" => 2})
      })
      
      job.should_receive(:delete).and_return(stub_deferrable(nil))
      
      strategy = lambda { |df, params|
        df.succeed_later_with(nil, 0.2)
      }
      
      jobrunner = Juggler::JobRunner.new(job, {}, strategy)
      jobrunner.run
      
      # To check that check_for_timeout doesn't timeout in the sucess case
      EM.add_timer(0.1) {
        jobrunner.check_for_timeout
      }
      
      jobrunner.bind(:succeeded) {
        @state = :succeeded
      }
      
      jobrunner.bind(:done) { 
        @state.should == :succeeded
        done
      }
    }
  end
  
  it "should release job for retry if exception calling strategy" do
    em(1) {
      job = mock(:job, {
        :jobid => 1, 
        :stats => stub_deferrable({"time-left" => 2, "delay" => 0})
      })
      
      job.should_receive(:release).with({:delay => 2}).
        and_return(stub_deferrable(nil))
      
      strategy = lambda { |df, params|
        raise 'strategy blows up'
      }
      
      jobrunner = Juggler::JobRunner.new(job, {}, strategy)
      jobrunner.run
      
      jobrunner.bind(:retried) {
        @state = :retried
      }
      
      jobrunner.bind(:done) { 
        @state.should == :retried
        done
      }
    }
  end
  
  it "should release job for retry and call exception handler if job deferrable fails with an exception" do
    asserts = 0

    em(1) {
      job = mock(:job, {
        :jobid => 1,
        :stats => stub_deferrable({"time-left" => 2, "delay" => 0})
      })

      job.should_receive(:release).with({:delay => 2}).
        and_return(stub_deferrable(nil))

      Juggler.exception_handler = lambda { |e|
        e.message.should == "FAIL"
        asserts += 1
      }

      strategy = lambda { |df, params|
        EM.next_tick {
          df.fail(RuntimeError.new("FAIL"))
        }
      }

      jobrunner = Juggler::JobRunner.new(job, {}, strategy)
      jobrunner.run

      jobrunner.bind(:retried) {
        asserts += 1
      }

      jobrunner.bind(:done) {
        asserts.should == 2
        done
      }
    }
  end

  it "should release job for retry if job fails with no arguments" do
    em(1) {
      job = mock(:job, {
        :jobid => 1, 
        :stats => stub_deferrable({"time-left" => 2, "delay" => 0})
      })
      
      job.should_receive(:release).with({:delay => 2}).
        and_return(stub_deferrable(nil))
      
      strategy = lambda { |df, params|
        df.fail_later_with(nil)
      }
      
      jobrunner = Juggler::JobRunner.new(job, {}, strategy)
      jobrunner.run
      
      jobrunner.bind(:retried) {
        @state = :retried
      }
      
      jobrunner.bind(:done) { 
        @state.should == :retried
        done
      }
    }
  end
  
  it "should fail and delete job if job fails with :no_retry" do
    em(1) {
      job = mock(:job, {
        :jobid => 1, 
        :stats => stub_deferrable({"time-left" => 2, "delay" => 0})
      })
      
      job.should_receive(:delete).and_return(stub_deferrable(nil))
      
      strategy = lambda { |df, params|
        df.fail_later_with(:no_retry)
      }
      
      jobrunner = Juggler::JobRunner.new(job, {}, strategy)
      jobrunner.run
      
      jobrunner.bind(:failed) {
        @state = :failed
      }
      
      jobrunner.bind(:done) { 
        @state.should == :failed
        done
      }
    }
  end
  
  it "should retry job if strategy deferrable exceeds timeout" do
    em(1) {
      job = mock(:job, {
        :jobid => 1, 
        :stats => stub_deferrable({"time-left" => 1, "delay" => 0})
      })
      
      job.should_receive(:release).with({:delay => 2}).
        and_return(stub_deferrable(nil))
      
      strategy = lambda { |df, params|
        df.succeed_later_with(nil, 2)
      }
      
      jobrunner = Juggler::JobRunner.new(job, {}, strategy)
      jobrunner.run
      
      EM.add_timer(0.1) {
        jobrunner.check_for_timeout
      }
      
      jobrunner.bind(:timed_out) {
        @state = :timed_out
      }
      
      jobrunner.bind(:done) { 
        @state.should == :timed_out
        done
      }
    }
  end
end
