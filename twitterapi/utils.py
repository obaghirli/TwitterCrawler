import oauth2 
import sys
import threading
import httplib2

from pymongo import MongoClient
from pymongo.errors import PyMongoError

import settings
import utils

import errors as apiErrors
import logging
import json
import pprint
import time

def take_note_on_rate_limits(wake_up_time, end_point):
	with open("depleted_endpoints.txt","a") as file:
		file.write( end_point+" "+str(wake_up_time)+"\n" )


def safe_mode(header, end_point):

	try:
		if int(header['x-rate-limit-remaining']) <= 2:
			current_unix_time=int(time.time())
			reset_unix_time=int(header['x-rate-limit-reset'])
			safety_factor=2
			sleep_time=reset_unix_time-current_unix_time+safety_factor
			logging.warning("[%s] Sleeping in safe mode for %s seconds" %(end_point, str(sleep_time)) )
			print 			"[%s] Sleeping in safe mode for %s seconds" %(end_point, str(sleep_time))
			take_note_on_rate_limits(reset_unix_time+safety_factor, end_point)
			time.sleep(sleep_time)
			file=open("depleted_endpoints.txt","w").close()
			logging.warning("[%s] End Point is now clear. Ready to go!" %end_point)
			print 			"[%s] End Point is now clear. Ready to go!" %end_point

	except Exception as e:
		if "x-rate-limit-remaining" not in header:
			pass
		else:
			logging.error("Unknown Error [safe_mode]")
			raise e

def check_depleted_endpoints():
	logging.warning("-"*10+"Checking for Depleted Endpoints")
	print 		"\n"+"-"*10+"Checking for Depleted Endpoints"
	wake_up_times=[]
	end_points=[]
	try:
		with open("depleted_endpoints.txt",'r') as file:
			current_unix_time=int(time.time())
			line=file.readline().strip()
			while len(line)!=0:
				[end_point, wake_up_time]=line.split()
				wake_up_times.append(int(wake_up_time)-current_unix_time)
				end_points.append(end_point)
				line=file.readline().strip()
	except IOError as e:
		if e.errno  == 2:
			pass

	if len(end_points) > 0:
		max_wait_time=sorted(wake_up_times, reverse=True)[0]
		if max_wait_time<0:
			file=open("depleted_endpoints.txt",'w').close()
			logging.warning("All End Points are clear. Ready to go!")
			print 			"All End Points are clear. Ready to go!"
		else:
			logging.warning( "Depleted End Points: {0}".format( ", ".join(end_points) ) )
			print 			 "Depleted End Points: {0}".format( ", ".join(end_points) )
			logging.warning("Please wait %s seconds untill All End Points clear ..." %str(max_wait_time))
			print 			"Please wait %s seconds untill All End Points clear ..." %str(max_wait_time)
			logging.warning("Sleeping in safe mode for %s seconds" %str(max_wait_time) )
			print 			"Sleeping in safe mode for %s seconds" %str(max_wait_time)
			time.sleep(max_wait_time)
			file=open("depleted_endpoints.txt",'w').close()
			logging.warning("All End Points are now clear. Ready to go!")
			print 			"All End Points are now clear. Ready to go!"
	else:
		logging.warning("All End Points are clear. Ready to go!")
		print 			"All End Points are clear. Ready to go!"



def is_complete_source(operation, mongo_client):
	collection_handler = mongo_client['History']['history_source']
	response = collection_handler.find_one({'operation':operation})
	if response is not None: #not first run
		if response['status'] == 'incomplete':
			return { 'db_name':response['db_name'], 'track_item':response['track_item'], 'pageNum':response['pageNum'], 'counter':response['counter']  }
		elif response['status'] == 'complete':
			return "complete"
	else:
		return "first_run" #first run


def is_complete_followers(operation, mongo_client):
	collection_handler = mongo_client['History']['history_followers']
	response = collection_handler.find_one({'operation':operation})
	if response is not None: #not first run
		if response['status'] == 'incomplete':
			return { 'db_name':response['db_name'], 'source_index':response['source_index'], 'cursor_counter':response['cursor_counter'], 'id_index':response['id_index']  }
		elif response['status'] == 'complete':
			return "complete"
	else:
		return "first_run" #first run

