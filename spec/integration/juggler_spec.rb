require File.expand_path('../../spec_helper', __FILE__)

describe "Juggler" do
  include EM::SpecHelper
  
  before :each do
    # Reset state
    Juggler.instance_variable_set(:@connection, nil)
    Juggler::Runner.instance_variable_set(:@runners, [])
    
    # Start clean beanstalk instance for each test
    Juggler.server = "beanstalk://localhost:10001"
    system "beanstalkd -p 10001 &"
    sleep 0.1
  end
  
  after :each do
    # TODO: Use pid
    system "killall beanstalkd"
  end
  
  it "should successfully excecute one job" do
    em(1) do
      params_for_jobs_received = []
      Juggler.juggle(:some_task, 1) { |df, params|
        params_for_jobs_received << params
        df.succeed_later_with(nil)
      }
      Juggler.throw(:some_task, {:some => "params"})
      
      EM.add_timer(0.1) {
        params_for_jobs_received.should == [{:some => "params"}]
        done
      }
    end
  end
  
  it "should run correct number of jobs concurrently" do
    em(1) do
      params_for_jobs_received = []
      Juggler.juggle(:some_task, 2) { |df, params|
        params_for_jobs_received << params
        df.succeed_later_with(nil, 0.2)
      }
      
      10.times { |i| Juggler.throw(:some_task, i) }
      
      EM.add_timer(0.3) {
        # After 0.3 seconds, 2 jobs should have completed, and 2 more started
        params_for_jobs_received.should == [0, 1, 2, 3]
        done
      }
    end
  end
  
  it "should stop em after jobs completed when using Juggler.stop" do
    job_finished = false
    job_started = false
    em(1) do
      Juggler.juggle(:some_task, 1) { |df, params|
        job_started = true
        EM.add_timer(0.2) {
          job_finished = true
          df.succeed
        }
      }
      Juggler.throw(:some_task, 'foo')
      EM.add_timer(0.1) {
        job_started.should == true
        job_finished.should == false
        Juggler.stop
      }
    end
    job_finished.should == true
  end
  
  it "should kill jobs that do not complete within shutdown_grace_timeout" do
    job_finished = false
    job_started = false
    em(1) do
      Juggler.shutdown_grace_timeout = 0.1
      Juggler.juggle(:some_task, 1) { |params|
        dd = EM::DefaultDeferrable.new
        job_started = true
        EM.add_timer(1) {
          job_finished = true
          dd.succeed
        }
        dd
      }
      Juggler.throw(:some_task, 'foo')
      EM.add_timer(0.1) {
        job_started.should == true
        job_finished.should == false
        Juggler.stop
      }
    end
    job_finished.should == false
  end
end