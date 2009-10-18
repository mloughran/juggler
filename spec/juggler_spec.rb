require File.dirname(__FILE__) + '/spec_helper'

describe Juggler do
  it "should put jobs on queue" do
    Juggler.throw('method', {:foo => "bar"})
  end
end
