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
      
      strategy = lambda {
        stub_deferrable(nil, 0.2)
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
      
      strategy = lambda {
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
  
  it "should fail and delete job if strategy deferrable fails with no arg" do
    em(1) {
      job = mock(:job, {
        :jobid => 1, 
        :stats => stub_deferrable({"time-left" => 2})
      })
      
      job.should_receive(:delete).and_return(stub_deferrable(nil))
      
      strategy = lambda {
        stub_failing_deferrable(nil)
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
  
  it "should retry job if strategy deferrable fails with :retry" do
    em(1) {
      job = mock(:job, {
        :jobid => 1, 
        :stats => stub_deferrable({"time-left" => 2, "delay" => 0})
      })
      
      job.should_receive(:release).with({:delay => 2}).
        and_return(stub_deferrable(nil))
      
      strategy = lambda {
        stub_failing_deferrable(:retry)
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
  
  it "should retry job if strategy deferrable exceeds timeout" do
    em(1) {
      job = mock(:job, {
        :jobid => 1, 
        :stats => stub_deferrable({"time-left" => 1, "delay" => 0})
      })
      
      job.should_receive(:release).with({:delay => 2}).
        and_return(stub_deferrable(nil))
      
      strategy = lambda {
        stub_deferrable(nil, 2)
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
