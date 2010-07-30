$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

require 'juggler'

require 'rubygems'
require 'em-spec/rspec'

module EM
  class ChainableDeferrable
    include Deferrable

    def callback(&block)
      super
      self
    end

    def errback(&block)
      super
      self
    end
  end
end

def stub_deferrable(callback, time = 0.01)
  d = EM::ChainableDeferrable.new
  EM.add_timer(time) {
    d.succeed(callback)
  }
  d
end

def stub_failing_deferrable(callback, time = 0.01)
  d = EM::ChainableDeferrable.new
  EM.add_timer(time) {
    d.fail(callback)
  }
  d
end
