# Simple serverless services for Google AppEngine

Say, we have a Google AppEngine application written in Python with the following lambda-like
function that takes a number of zipcodes and looks up their address:

    def geolocate(zipcodes):
        return [get_address(zipcode) for zipcode in zipcodes]

Now assume that the get_address function itself is implemented as a service call to
an external geolocation service, such as the Google Maps API. We now run into the bottleneck
of network speed. Depending on the distance between the client and the server, each call
realistically costs ~10ms if server and client are in the same region and ~75ms between NYC
and Seattle. Crossing the Atlantic Ocean takes an additional 100ms.

If we have to resolve 1,000 addresses, the above simple code takes 10s at best and 3 minutes at
worst. Typically, APIs are aware of this issue and allow for bundling of requests into one.
If such bundling is not available, the solution would be to run multiple requests in parallel,
either by using multiple threads or separate processes.

The above example places the bottleneck in the network. However, for different calculations, the
bottleneck might be CPU-heavy and require load balancing to a number of servers in a cluster.
Managing such a cluster requires extra planning. It would be nice if we could not worry about
how the calls are made in parallel, and simply use a decorator as follows:

    @serverless.parallel
    def geolocate(zipcodes):
        # run in parallel on a cluster
        return [get_address(zipcode) for zipcode in zipcodes]

What the decorator does in this case is to split up the data into smaller chunks, send each chunk
to a different server, and collate the results as they arrive in parallel, and finally return the
result. For the reader, this code looks like it runs serially in one thread, but in reality it
runs highly parallelized.

Before we get to how this is implemented, here is a typical run of the hosted application at
[simple-serverless.appspot.com](http://simple-serverless.appspot.com):

    *Serverless Pipeline Demo*
    Number of zipcodes = 3000
    Regular duration = 345.57s
    Parallel duration = 18.98s
    Speedup = 18.2X
    Details per pipeline step:
      1 - Step "geolocate" took 18.65s for 100 workers and 3000 elements
      2 - Step "cleanup" took 0.34s for 100 workers and 3000 elements
      3 - Step "sort" took 0.00s for 1 worker and 413 elements

In the above run, we resolved 3,000 zipcodes in ~19 seconds, while normal
execution would take almost 6 minutes. Actual speedup depends on how many nodes
are currently warmed up. Typically, a second run runs faster.

# Implementation

To implement the above decorator, the easy part is splitting the data into smaller chunks
and collating the results after all the work is done. The hard part is finding servers to run the stateless
function on and use an effective load balancer to horizontally scale to an elastic demand. It
turns out that Google AppEngine was designed to do exactly that.

If we can somehow handle a chunk by making a web service call back to our own domain,
using GAE load balancing to dispatch back to the current server, or wake a new one
depending on current load, we effectively piggyback on GAE to create a poor-man's 
serverless lambda implementation. It turns out that is not that hard to do.

Breaking up the original data into multiple chunks looks like this:

    bucketSize = max(1, int(len(data) / WORKER_COUNT))
    buckets = [
        data[n: n + bucketSize]
        for n in xrange(0, len(data), bucketSize)
    ]
    
We then convert the chunk into JSON, encode what lambda function we want to run, and
invoke it as an RPC call:

    # create a non-blocking, asynchronous worker to handle one bucket, on our
    # own instance, or on new ones automatically launched by appengine
    def createWorker(bucket):
        worker = urlfetch.create_rpc(deadline=60)
        scheme = os.environ['wsgi.url_scheme']
        host = os.environ['HTTP_HOST']
        url = scheme + '://' + host + URL
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            SECRET_KEY: SECRET,
        }
        payload = urllib.urlencode({
            'module': method.__module__,
            'className': className,
            'isclass': isclass,
            'method': method.__name__,
            'data': json.dumps(bucket),
            'args': json.dumps(args),
            'argv': json.dumps(argv),
        })
        return urlfetch.make_fetch_call(worker, url, payload, 'POST', headers)

Note: We use a secret that is known only to the application, to avoid external calls
to our worker functions. This security by obscurity is not recommended for 
production applications and a stronger authentication model should be used then.

The receiving worker route is set up in the client code as follows:

    app = webapp2.WSGIApplication([
        ('/', ZipCodeHandler),
        serverless.init('/serverless_route', SERVERLESS_SECRET)

We initialize serverless with a route name of our choice, which can be any arbitrary name
and the secret of our choosing. When a chunk of data is sent to that route as a POST, it is 
unpacked, the requested lambda function is invoked, and the result is returned in JSON
format.

The results for each of the workers is collated as follows:

    result = list(itertools.chain(*map(getResult, map(createWorker, buckets))))
    
The workers are created and will invoke their lambda functions asynchronously using
the _createWorker_ function shown above. The result
is collected in the _getResult_ handler, which blocks until the result is received:

    def getResult(worker):
        response = worker.get_result().content
        try:
            return json.loads(response)
        except Exception as e:
            logging.error('Error: %s' % e)
            return [e]

An additional utility is offered to handle workflow processing in the form of a pipeline:

    pipeline = serverless.Pipeline(geolocate, cleanup, sort)
    
This sets up a serverless pipeline where data streams from _geolocate_, to _cleanup_, to _sort_. 
The pipeline is invoked using its _run_ method:

    zipcodes = [random.randint(10000,99999) for n in xrange(ZIPCODE_COUNT)]
    addresses = pipeline.run(zipcodes)

The _cleanup_ and _sort_ functions look similar to the _geolocate_ function. Key is that
they are fully "functional". They depend only on the values of the data they process and
are completely context free. The _cleanup_ function is executed in parallel:

    @serverless.parallel
    def cleanup(addresses):
        return filter(None, addresses)

The _sort_ function is executed sequentially, in the current thread, on all the collated
results from the previous step in the pipeline:

    @serverless.sequential
    def sort(addresses):
        return sorted(addresses)
        
The entire implementation of this simple serverless library is just 230 lines of Python,
slightly more than the length of this README file. 

*Disclaimer*: This work is a personal weekend project and is unrelated to [Google Cloud 
Functions](https://cloud.google.com/functions), a serverless functions solution based on
Node.js and [Google Dataflow for
Python](https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python),
a more comprehensive solution for data-driven distribubted programming pipelines.