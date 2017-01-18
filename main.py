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

""" This is a sample application that uses the serverless pipeline API.

First, we generate a number of random zipcodes. We then create a pipeline to
resolve them to actual addresses using the maps.googleapis.com geocode API.
For example, the zipcode "40057" resolves to address "Pleasureville, KY 40057, USA".

The serverless.parallel and serverless.sequential decorators work on any function
that accepts a list of elements and produces a list of elements. The function has to
be context-free, so that the function can be executed in any order and on any server.
"""

import json
import logging
import random
import serverless
import time
import urllib
import webapp2

SIMULATED_EXTRA_LATENCY = 0.05
ZIPCODE_COUNT = 3000


@serverless.parallel
def geolocate(zipcodes):
    # run in parallel on a cluster
    return [get_address(zipcode) for zipcode in zipcodes]


@serverless.parallel
def cleanup(addresses):
    # run in parallel on a cluster
    return filter(None, addresses)


@serverless.sequential
def sort(addresses):
    # run in one thread in the current process
    return sorted(addresses)

pipeline = serverless.Pipeline(geolocate, cleanup, sort)


class ZipCodeHandler(webapp2.RequestHandler):
    def get(self):
        # Create a number of random zipcodes.
        zipcodes = [random.randint(10000,99999) for n in xrange(ZIPCODE_COUNT)]

        # Convert the zipcodes to addresses using a microservice pipeline.
        addresses = pipeline.run(zipcodes)

        # Calculate how long it would take without a serverless pipeline
        actual_duration = sum(duration for _,duration,_ in addresses)
        extra_durantion = SIMULATED_EXTRA_LATENCY * ZIPCODE_COUNT
        normal_duration = actual_duration + extra_durantion
        duration = sum(f.duration for f in pipeline.steps)

        # Produce output in HTML.
        self.response.write('<b>Serverless Pipeline Demo</b><p>')
        self.response.write('Number of zipcodes = %d<br>' % ZIPCODE_COUNT)
        self.response.write('Regular duration = %.2fs<br>' % normal_duration)
        self.response.write('Parallel duration = %.2fs<br>' % duration)
        self.response.write('Speedup = %.1fX<p>' % (normal_duration / duration))
        self.response.write('Details per pipeline step:<ol>')
        for step in pipeline.steps:
            self.response.write('<li>Step "%s" took %.2fs for %d worker%s and %d elements' % (
                step.__name__, step.duration, step.workers,
                's' if step.workers > 1 else '', step.count
            ))
        self.response.write('</ol>')
        self.response.write('<br>'.join([
            '%s => %s' % (zipcode,address) for zipcode,_,address in addresses
        ]))


def get_address(zipcode):
    try:
        start = time.time()
        url = 'http://maps.googleapis.com/maps/api/geocode/json?address=%d'
        response = json.loads(urllib.urlopen(url % zipcode).read())
        time.sleep(SIMULATED_EXTRA_LATENCY) # simulate extra delay
        end = time.time()
        return zipcode, end-start, response['results'][0]['formatted_address']
    except Exception as e:
        logging.error('Cannot resolve zipcode %s: %s' % (zipcode, e))
        return None


app = webapp2.WSGIApplication([
    ('/', ZipCodeHandler),
    serverless.init('/serverless', '27d9d75205fed75d3499bd872f237176')

], debug=True)
