# A simple serverless service implementation using Google AppEngine

Run stateless microservice functions in parallel on Google AppEngine

Here is a typical run of the hosted application at [simple-serverless.appspot.com](http://simple-serverless.appspot.com):

    *Serverless Pipeline Demo*
    Number of zipcodes = 3000
    Regular duration = 345.57s
    Parallel duration = 18.98s
    Speedup = 18.2X
    Details per pipeline step:
      1 - Step "geolocate" took 18.65s for 100 workers and 3000 elements
      2 - Step "cleanup" took 0.34s for 100 workers and 3000 elements
      3 - Step "sort" took 0.00s for 1 worker and 413 elements

This demo uses Google geocoding service calls to resolve zipcodes to addresses.
Each call is really fast, as the app and the geocoding service are colocated
in the same datacenter. Therefore we add a small simulated latency of 50ms,
to mimic the job taking some work.

In the above run, we resolved 3,000 zipcodes in ~19 seconds, while normal
execution would take almost 6 minutes. Actual speedup depends on how many nodes
are current warmed up. A second run typically runs faster.

Execution speedup depends on the number of workers, how long the longest
chunk takes, and load balancing imposed by GAE.
