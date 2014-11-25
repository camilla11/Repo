import os
import json
import redis
import time
import thread
from threading import Thread
from multiprocessing import Process
import subprocess

def main():

	#f = open("random_numbers.json", 'r+')
	#contents = f.read()
	#contents = contents.replace("'", '"')
	#f.seek(0)
	#f.write(contents)
	#f.close()
	
	r = redis.StrictRedis(host='localhost', port=6379, db=0)
	data_file =  open("random_numbers.json") 
	data = json.load(data_file)
	
	r.flushall()
	parsepattern(r)
	print("Pattern is parsed - begin matching\n")
	
	
	'''
	# First idea: add patterns to redis one by one (this is not time consuming)
	# try to match entire pattern - if no match match less and less etc
	number_2_rate_dict = {}
	start = time.clock()
	for number in data:
		#so many iterations.. 
		numbertofind = number.keys()[0]
		#mget reduces latency
		array_of_rates = r.mget(numbertofind, numbertofind[:-1], numbertofind[:-2], numbertofind[:-3],
				numbertofind[:-4], numbertofind[:-5], numbertofind[:-6], numbertofind[:-7])
		while array_of_rates and None in array_of_rates: 
			#compared to redis access, getting longest match is taking the most time		
			array_of_rates.remove(None)	
		if not array_of_rates:
			# set as default rate
			number_2_rate_dict[numbertofind] = 0.10
		else:
			number_2_rate_dict[numbertofind] = array_of_rates[0]

	print time.clock() - start 
	
	# Second idea: store patterns in redis in some structure based on area code (try hash)
	# access the structure and then select further based on next number
	number_2_rate_dict = {}
	start = time.clock()
	for number in data:
		#so many iterations.. 
		numbertofind = number.keys()[0]
		#hgetall reduces latency
		dict_of_rates = r.hgetall(numbertofind[0:3])
		newnumbertofind = numbertofind
		while dict_of_rates and newnumbertofind and newnumbertofind not in dict_of_rates: 
			#compared to redis access, getting longest match is taking the most time		
			newnumbertofind = newnumbertofind[:-1]
		if not newnumbertofind or not dict_of_rates:
			# set as default rate
			number_2_rate_dict[numbertofind] = 0.10
		else:
			number_2_rate_dict[numbertofind] = dict_of_rates[newnumbertofind]

	print time.clock() - start 
	'''
	
	# After this , how to take advantage of redis parallelism? what is redis parallelism?
	# each redis server is single threaded so if i want to run parallel mgets i need to 
	# have multiple servers - however I can run multiple clients on one server, it just 
	# queues up the requests? Since the actual longest match takes much more time than the redis
	# access - create parallelism for the matching 
	startglobal = time.clock()
	'''t1 = Thread(target=match, args=(data, r, 0, len(data)/2))
	t2 = Thread(target=match, args=(data, r, len(data)/2, len(data)))	
	t1.start()
	t2.start()
	t1.join()
	t2.join()''' #This has GIL problem - use real multiprocessing
	p1 = Process(target=match, args=(data, r, 0, len(data)/2))
	p2 = Process(target=match, args=(data, r, len(data)/2, len(data)))
	p1.start()
	p2.start()
	p1.join()
	p2.join()
	print "Total time of two threads:", time.clock() - startglobal
	
	data_file.close()	
		
def match(data, r, startindex, finishindex):
	number_2_rate_dict = {}
	start = time.clock()
	for number in data[int(startindex):int(finishindex)]:
		#so many iterations.. 
		numbertofind = number.keys()[0]
		#hgetall reduces latency
		dict_of_rates = r.hgetall(numbertofind[0:3])
		newnumbertofind = numbertofind
		while dict_of_rates and newnumbertofind and newnumbertofind not in dict_of_rates: 
			#compared to redis access, getting longest match is taking the most time		
			newnumbertofind = newnumbertofind[:-1]
		if not newnumbertofind or not dict_of_rates:
			# set as default rate
			number_2_rate_dict[numbertofind] = 0.10
		else:
			number_2_rate_dict[numbertofind] = dict_of_rates[newnumbertofind]
	print time.clock() - start 
		
		
def parsepattern(r):
	pattern_file = open("patterns.txt")
	patterns = pattern_file.readlines()
	for line in patterns:
    # parse input, assign values to variables
		if not line.strip().startswith("'"):
			continue
		key, value = line.split(":")
		value = value.strip()[:-1]
		key = key.strip()[1:-1]
		if "x" in key:
			key = key.split("x")[0]
		#r.set(key, value)
		r.hmset(key[0:3], {key:value})
	pattern_file.close()

	
	
if __name__ == "__main__":
	main()
