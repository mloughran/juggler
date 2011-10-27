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

The job is considered to have failed in the following cases:

* If the block raises an error
* If the block fails the passed deferrable
* If the job timeout is exceeded. In this case the passed deferrable will be failed by juggler. If you need to clean up any state in this case (for example you might want to cancel a HTTP request) then you should bind to `df.errback`.

Important points to note:

* If your deferrable code raises errors, this will not be handled by juggler.
