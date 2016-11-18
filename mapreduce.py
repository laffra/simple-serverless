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

""" This is a library that implements a simple mapreduce API.

"""

__author__ = """laffra@google.com (Chris Laffra)"""

import __builtin__
import httplib
import inspect
import itertools
import json
import logging
import os
import time
import urllib
import urllib2
import webapp2

from functools import wraps

from google.appengine.api import urlfetch


SECRET_KEY = 'PPP-Secret'
SECRET = None
URL = None
WORKER_COUNT = 100

ROUTE_ERROR = 'No route specified. Did you forget to call mapreduce.init()?'
DATA_NOT_A_LIST = 'First argument to %s should be a list, not %s'
DATA_MISSING = 'Expected at least one argument to %s'

class ParallelHandler(webapp2.RequestHandler):
    '''
    Handler for asynchronous worker requests consisting of a method to call
    and a bucket of elements to work on. Supported method is POST.
    
    Request arguments:
	module String The name of the module to load the method from
	class String The name of the class to load the method from
	method String The name of the method
	data JSON-list The elements to process in parallel
	args JSON-list Positional arguments to be passed the method
	argv JSON-dict Keyword arguments to be passed the method
    '''
    def post(self):
	# accept only workers started from the current app
    	if self.request.headers.get(SECRET_KEY) != SECRET:
	    self.response.set_status(404)
	    return

	# extract the method details and data chunk to run
        moduleName = self.request.get('module')
	className = self.request.get('className')
	isclass = bool(self.request.get('isclass'))
	methodName = self.request.get('method')
	data = json.loads(self.request.get('data'))
        args = json.loads(self.request.get('args'))
        argv = json.loads(self.request.get('argv'))

	# dynamically load the method
        module = __import__(moduleName)
	if className:
	    clazz = getattr(module, className)
	    instance = clazz if isclass else clazz()
	    method = getattr(instance, methodName)
	    wrapped = getattr(method, '__wrapped__')
	    # run the wrapped method as instance/class method
	    result = wrapped(instance, data, *args, **argv)
	else:
	    method = getattr(module, methodName)
	    wrapped = getattr(method, '__wrapped__')
	    # run the wrapped method as top-level function
	    result = wrapped(data, *args, **argv)

	# produce the JSON result
	self.response.write(json.dumps(result))


def parallel(method):
    '''
    A decorator that speeds up a given method by running it in parallel. 
    This is done by splitting up the original data passed as the first
    argument of the method into smaller buckets. The original method is then
    executed on each bucket using a new appengine request per bucket. 

    This decorator looks like mapreduce, but rather than use a dedicated cluster
    or worker machines, it (ab)uses the built-in horizontal scaling capability of 
    appengine to allocate new workers on demand. As a result, it requires no 
    configuration at all.

    As this approach increases the number of calls made to your appengine service,
    this impacts your appengine quotas. See https://console.cloud.google.com for
    more details.
    '''

    @wraps(method)
    def parallel_implementation(*args, **argv):
	# handle bad setup
	if not SECRET:
	    error = ROUTE_ERROR
	    parallel_implementation.stats = error
	    logging.error(error)
	    raise RuntimeError(error)

	if len(args) < 1:
	    error = DATA_MISSING % method.__name__
	    raise TypeError(error)

	if isinstance(args[0], list):
	    self = None
	    isclass = False
	    className = ''
	    data = args[0]
	    args = list(args)[1:]
	else:
	    isclass = inspect.isclass(args[0])
	    className = args[0].__name__ if isclass else args[0].__class__.__name__
	    self = args[0]
	    data = args[1]
	    args = list(args)[2:]

	if not isinstance(data, list):
	    error = DATA_NOT_A_LIST % (method.__name__, type(data).__name__)
	    parallel_implementation.stats = error
	    logging.error(error)
	    raise TypeError(error)

	# initialization of starting time and chunk size
	start = time.time()
	bucketSize = max(1, int(len(data) / WORKER_COUNT))

	# split the work into smaller buckets, each to be handled in parallel
	buckets = [
	    data[n : n + bucketSize]
	    for n in xrange(0, len(data), bucketSize)
	]

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

	# extract the result from a given worker. This is a blocking call
	def getResult(worker):
	    response = worker.get_result().content
	    try:
		return json.loads(response)
	    except Exception as e:
		logging.error('Error: %s' % e)
	        return [ e ]

	# create a worker for each bucket, and then gather the results
	map = __builtin__.map
	result = list(itertools.chain(*map(getResult, map(createWorker, buckets))))

	# generate statistics
	duration = time.time() - start
	workers = len(data) / bucketSize
	parallel_implementation.stats = '%d workers, took %.2fs' % (workers, duration)

	# return the processed chunks that are now joined together again
	return result

    setattr(parallel_implementation, '__wrapped__', method)
    return parallel_implementation

map = reduce = parallel

def init(url, secret):
    global SECRET, URL
    URL = url
    SECRET = secret
    return (url, ParallelHandler)
