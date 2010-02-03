require 'rubygems'
require 'sinatra'

get '/slow' do
  sleep 3
  return 'Finally done'
end

get '/fast' do
  sleep 1
  return 'Fast done'
end
