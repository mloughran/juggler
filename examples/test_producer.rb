require 'rubygems'
$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'juggler'

# Throw some jobs

# EM.run {
#   # 10.times do |i|
#   #   path = ['/fast', '/slow'][i % 2]
#   #   Juggler.throw(:http, path, :ttr => 40)
#   # end
#   1.times { Juggler.throw(:timer, {:foo => 'bar'}, :ttr => 5) }
# }

Thread.new do
  EM.run
end

loop do
  puts "Choose your ttr!"
  time = gets.to_i
  puts "Creating job with ttr #{time}"
  EM.next_tick {
    Juggler.throw(:timer, {:foo => 'bar'}, :ttr => time)
  }
end