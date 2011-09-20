Add jobs for asynchronous processing

    Juggler.throw(:method, params)

Add handlers, with optional concurrency, inside an EM loop

    EM.run {
      Juggler.juggle(:method, 10) do |deferrable, params|
        # Succeed the deferrable when the job is done
      end
    }

For example

    Juggler.juggle(:download, 10) do |df, params|
      http = EM::Protocols::HttpClient.request({
        :host => params[:host], 
        :port => 80, 
        :request => params[:path]
      })
      http.callback do |response|
        puts "Got response status #{response[:status]} for #{a}"
        df.success
      end
    end

Important points to note:

* If your deferrable code raises errors, this will not be handled by juggler.
