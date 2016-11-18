# gae-map

A simple map (without reduce) work distributor for Google AppEngine

Here is a typical run of the hosted application at [simple-mapreduce.appspot.com](http://simple-mapreduce.appspot.com/?workers=400&count=3000&latency=0.05):

    Number of zipcodes = 3000
    Addition simulated latency = 0.05s
    428 workers, took 4.20s
    Regular duration = 150.15s
    Parallel duration = 5.59s
    Speedup = 26.9X

It uses Google geocoding service calls to resolve zipcodes to addresses. Each call is really fast, as the app and the geocoding service are colocated in the same datacenter. Therefore we add a small simulated latency of 50ms, to mimic the job taking some work.

In the above run, we resolved 3,000 zipcodes in less than 6 seconds, while normal execution would take 2,5 minutes.

Execution speedup depends on the number of workers, how long the longest chunk takes, and load balancing imposed by GAE.
