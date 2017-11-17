import oauth2 
import sys
import threading
import httplib2
import re

from pymongo import MongoClient
from pymongo.errors import PyMongoError

import settings
import utils

import errors as apiErrors
import logging
import json
import pprint
import time



locations={
				'Turkey':		['Turkey','Istanbul','Ankara','Izmir','Bursa', 'Adana','Antalya','Sakarya','Trabzon','Konya','Kayseri','Samsun'],
				'Italy':		['Italy','Rome','Milan','Naples','Turin','Palermo','Genoa','Bologna','Florence','Bari','Catania','Venice','Verona','Messina','Padua','Trieste','Taranto'],
				'United States':['United States',',\s*AL$',',\s*AK$',',\s*AZ$',',\s*AR$',',\s*CA$',',\s*CO$',',\s*CT$',',\s*DE$',',\s*FL$',',\s*GA$',',\s*HI$',',\s*ID$',',\s*IL$',',\s*IN$',',\s*IA$',',\s*KS$',
										',\s*KY$',',\s*LA$',',\s*ME$',',\s*MD$',',\s*MA$',',\s*MI$',',\s*MN$',',\s*MS$',',\s*MO$',',\s*MT$',',\s*NE$',',\s*NV$',',\s*NH$',',\s*NJ$',',\s*NM$',',\s*NY$',',\s*NC$',
										',\s*ND$',',\s*OH$',',\s*OK$',',\s*OR$',',\s*PA$',',\s*RI$',',\s*SC$',',\s*SD$',',\s*TN$',',\s*TX$',',\s*UT$',',\s*VT$',',\s*VA$',',\s*WA$',',\s*WV$',',\s*WI$',',\s*WY$',',\s*DC$',
										'Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','Delaware','Florida','Georgia','Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas',
										'Kentucky','Louisiana','Maine','Maryland','Massachusetts','Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire', 'New Jersey',
										'New Mexico','New York','North Carolina','North Dakota','Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island','South Carolina','South Dakota','Tennessee','Texas',
										'Utah','Vermont','Virginia','Washington','West Virginia','Wisconsin','Wyoming','District of Columbia'],
				'Russia':		['Russia','Moscow','Saint Petersburg','Novosibirsk','Yekaterinburg','Rostov','Omsk','Kazan', 'Belgorod','Novgorod','Kaliningrad','Nizhnevartovsk'],
				'France':		['France','Paris','Marseille','Lyon','Toulouse','Nice','Nantes','Strasbourg','Montpellier','Bordeaux','Lille','Rennes','Reims','Le Havre','Saint-Etienne','Toulon','Orleans','Tours']
		}




class Database(object):
	def __init__(self, db_ip, db_port):
		self.db_ip=db_ip
		self.db_port=db_port
		self.client=self.connect()

	def connect(self):
		try:
			mongo_client=MongoClient( self.db_ip, self.db_port )
			mongo_client.database_names()
			#print 			"\nMongoDB: started at: %s:%s" %( self.db_ip, self.db_port )
			return mongo_client

		except PyMongoError as e:
			logging.error(str(e))
			raise e

		except Exception as e:
			logging.error("Unknown Error: Database Start-Up")
			raise e


class Statistics(object):
	def __init__(self, track_settings, mongo_client):
		self.track_settings=track_settings
		self.db_names=self.get_db_names(self.track_settings)
		self.mongo_client=mongo_client
		self.output=self.output_initializer(self.db_names)
		self.general_info(self.db_names, self.mongo_client, self.output)
		self.by_location_frequency(self.db_names, self.mongo_client, self.output)



	def get_db_names(self, track_settings):
		return [ track_obj['db_name'] for track_obj in track_settings ]


	def output_initializer(self, db_names):
		output = {'db_names':[]}
		for db_name in db_names:
			db_name_module = {db_name:{'location_frequency': None, 'total_users_count':None, 'users_with_location':None }}
			output['db_names'].append(db_name_module)
		return output


	def general_info(self, db_names, mongo_client, output):
		print "-"*10, "General Info"
		for index, db_name in enumerate(db_names):
			print "general info:", db_name
			followers_handler = mongo_client[db_name]['Followers']
			total_users_count = followers_handler.find().count()
			users_with_location = followers_handler.find( {'$or':[{'location':{'$ne':''}},{'status.geo':{'$ne':None}}] }).count()
			output['db_names'][index][db_name]['total_users_count'] = str(total_users_count)
			try:
				output['db_names'][index][db_name]['users_with_location'] = [str(users_with_location), str(float(users_with_location)/total_users_count*100.0)+" %"]
			except ZeroDivisionError:
				output['db_names'][index][db_name]['users_with_location'] = [str(0), str(0)+" %"]
		print "\n"


	def by_location_frequency(self, db_names, mongo_client, output):
		print "-"*10, "By Location Frequency"
		for index, db_name in enumerate(db_names):
			followers_handler = mongo_client[db_name]["Followers"]
			total_users_count = followers_handler.find().count()
			location_frequency_module = {}
			locations_keys=locations.keys()
			
			for location_key in locations_keys:
				print "by location frequency:", db_name, location_key
				if location_key in ['United States']:
					regex = re.compile( "|".join(locations[location_key]) )
				else:
					regex = re.compile( "|".join(locations[location_key]), re.IGNORECASE )
				user_count_per_location = followers_handler.find( {"$or":[ {'location': regex}, {'status.place.country': regex} ]} ).count()
				try:
					location_frequency_module[location_key] = [str(user_count_per_location), str(float(user_count_per_location)/total_users_count*100.0)+" %"]
				except ZeroDivisionError:
					location_frequency_module[location_key] = [str(0), str(0)+" %"]


			output['db_names'][index][db_name]['location_frequency'] = location_frequency_module
			print "\n"







if __name__ == "__main__":

	try:
		print 			"\n"+"-"*10+"Statistics in Progress ..."
		mongodb_settings=settings.applications_settings.get("mongodb_settings")
		db=Database( mongodb_settings[0].get("db_ip"), mongodb_settings[0].get("db_port") )
		print "-"*36+"\n"
		track_settings = settings.applications_settings.get("track_settings")

		statistics = Statistics(track_settings, db.client)

		with open('statistics.json', 'w') as file:
			file.write( json.dumps( statistics.output, separators=(',',':'), indent=4 )  )


		print  			
		print 			"\n"+"-"*35+"\nFinished. All Done."

	except KeyboardInterrupt as e:
		logging.warning("Keyboard Interrupt\n")
		print str(e)
