require 'rubygems'
$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'juggler'

EM.run {
  Juggler.juggle(:http, 3) do |path|
    http = EM::Protocols::HttpClient.request({
      :host => "0.0.0.0",
      :port => 3000,
      :request => path
    })
    http.callback do |response|
      puts "Got response status #{response[:status]} and body \"#{response[:content]}\""
    end

    http
  end

  Juggler.juggle(:timer, 5) do |params|
    defer = EM::DefaultDeferrable.new

    EM::Timer.new(1) do
      defer.set_deferred_status :succeeded, nil
      # defer.set_deferred_status :failed, nil
    end

    defer.callback do
      puts "Timer ended (params #{params.inspect})"
    end

    defer.errback do
      puts "Timer failed"
    end

    defer
  end
}
