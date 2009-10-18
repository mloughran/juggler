Add jobs for asynchronous processing

    Juggler.throw(:method, params)

Add handlers, with optional concurrency, inside and EM loop

    EM.run {
      Juggler.juggle(:method, 10) do |params|
        # This code must return an eventmachine deferrable object
      end
    }

For example

    Juggler.juggle(:download, 10) do |params|
      http = EM::Protocols::HttpClient.request({
        :host => params[:host], 
        :port => 80, 
        :request => params[:path]
      })
      http.callback do |response|
        puts "Got response status #{response[:status]} for #{a}"
      end
      http
    end
