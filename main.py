#
# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" This is a sample application that uses the simple-mapreduce API.

It does so by generating random zipcodes and resolving them to actual addresses
using maps.googleapis.com geocode API. For example, the zipcode "40057" resolves
to address "Pleasureville, KY 40057, USA". The geocode API is quite fast, so
we can add extra simulated latency using a URL parameter, to resemble more realistic
workers. Moreover, the number zipcodes to resolve and the number of workers to use
are both parameters to the simulation.

The simple-mapreduce API is a decorator that works on any function that accepts 
a list as its first parameter. The function should be context-free. In other words,
the geolocate function declared in this example cannot refer to instance-specific
fields in "self". 
"""

__author__ = """laffra@google.com (Chris Laffra)"""


import json
import random
import time
import webapp2
import urllib

import mapreduce

GEOLOCATE = 'http://maps.googleapis.com/maps/api/geocode/json?address=%s'

class GeolocateHandler(webapp2.RequestHandler):
    def get(self):
	# Create a number of random zipcodes.
	count = int(self.request.get('count', 1000))
	latency = float(self.request.get('latency', 0.05))
	workers = int(self.request.get('workers', 100))
	zipcodes = [random.randint(10000,99999) for n in xrange(count)]
	start = time.time()

	mapreduce.WORKER_COUNT = workers  # override number of workers used

	# Resolve the zipcodes to addresses using a microservice.
	details = self.geolocate(zipcodes, latency)
	details = cleanup(details)

	# Create some statistics.
	parallelDuration = time.time() - start
	normalDuration = sum(duration for _,duration,_ in details) + latency * count
	ratio = normalDuration / parallelDuration

	# Produce output in HTML.
	self.response.write('''
		<hr>
		Number of zipcodes = %d<p>
		Addition simulated latency = %.2fs<p>
		%s<p>
		Regular duration = %.2fs<p>
		Parallel duration = %.2fs<p>
		Speedup = %.1fX
		<hr>
		<a href=?workers=%d&count=%d&latency=%.2f>Add more zipcodes</a>
		<a href=?workers=%d&count=%d&latency=%.2f>Double the simulated latency</a>
		<a href=?workers=%d&count=%d&latency=%.2f>Double the worker count</a>
		<hr>
		%s
	    ''' % (
		count,
		latency,
	        self.geolocate.stats,
		normalDuration,
		parallelDuration,
		ratio, 
	    	workers, count + 1000, latency,
	    	workers, count, latency * 2,
	    	workers * 2, count, latency,
		'<br>'.join(map(str, details))
	    )
	)


    @mapreduce.map
    def geolocate(self, zipcodes, latency):
	# Resolve a set of zipcodes into addresses. 
	time.sleep(latency * len(zipcodes))
	return [getAddress(zipcode) for zipcode in zipcodes]


def getAddress(zipcode):
    # Resolve a zipcode into an address. 
    # This makes a blocking microservice call to maps.googleapis.com.
    start = time.time()
    response = json.loads(urllib.urlopen(GEOLOCATE % zipcode).read())
    end = time.time()
    try:
	return (zipcode, end-start, response['results'][0]['formatted_address'])
    except:
	return None
    

@mapreduce.reduce
def cleanup(details):
    return filter(None, details)


app = webapp2.WSGIApplication([
    ('/', GeolocateHandler),
    mapreduce.init('/mapreduce', '27d9d75205fed75d3499bd872f237176')

], debug=True)
