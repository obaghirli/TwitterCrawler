import oauth2 
import sys
import threading
import httplib2
from socket import error as SocketError
import errno

from pymongo import MongoClient
from pymongo.errors import PyMongoError

import settings
import utils

import errors as apiErrors
import logging
import json
import pprint
import time

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s' ,filename='twitterapi.log', filemode='a', level=logging.WARNING)

class Database(object):
	def __init__(self, db_ip, db_port):
		self.db_ip=db_ip
		self.db_port=db_port
		self.client=self.connect()

	def connect(self):
		try:
			mongo_client=MongoClient( self.db_ip, self.db_port )
			mongo_client.database_names()
			logging.warning("MongoDB: started at: %s:%s" %( self.db_ip, self.db_port ) )
			print 			"\nMongoDB: started at: %s:%s" %( self.db_ip, self.db_port )
			return mongo_client

		except PyMongoError as e:
			logging.error(str(e))
			raise e

		except Exception as e:
			logging.error("Unknown Error: Database Start-Up")
			raise e


class TwitterApplication(object):
	def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
		self.authenticated=False
		self.consumer_key=consumer_key
		self.consumer_secret=consumer_secret
		self.access_token=access_token
		self.access_token_secret=access_token_secret
		self.authenticate()

	def authenticate(self):
		try:
			self.consumer = oauth2.Consumer(key=self.consumer_key, secret=self.consumer_secret)
			self.access_token = oauth2.Token(key=self.access_token, secret=self.access_token_secret)
			self.twitter_api = oauth2.Client(self.consumer, self.access_token)

			while True:
				while True:
					try:
						(header, data) = self.twitter_api.request("https://api.twitter.com/1.1/account/verify_credentials.json")
						utils.safe_mode(header, "account/verify_credentials")
						break
					except httplib2.ServerNotFoundError:
						logging.error("Unable to find api.twitter.com [authenticate]. Sleeping for 30 seconds ...")
						print 		  "Unable to find api.twitter.com [authenticate]. Sleeping for 30 seconds ..."
						time.sleep(30)
					except SocketError as e:
						if e.errno == errno.ECONNRESET:
							logging.error("Connection reset by peer [authenticate]. Sleeping for 30 seconds ...")
							print 		  "Connection reset by peer [authenticate]. Sleeping for 30 seconds ..."
							time.sleep(30)
						else:
							raise e



				if header['status'] != '503': # 503- Twitter API Overload
					break
				else:
					logging.warning("Server Overload [authenticate]. Sleeping for 30 seconds ...")
					print 			"Server Overload [authenticate]. Sleeping for 30 seconds ..."
					time.sleep(30)

			if header['status'] == '200':
				self.authenticated=True
				logging.warning("Twitter API: Authentication: Success!")
				print 			"Twitter API: Authentication: Success!"
			else:
				raise apiErrors.AuthError("Twitter API: Authentication: Failed!", header['status'], data['errors'] if 'errors' in data else data) 

		except apiErrors.AuthError as e: 
			logging.error(str(e))
			raise e
		except Exception as e:
			logging.error("Unknown Error: Authentication")
			raise e



class SourceCollector(object):
	def __init__(self,track_settings, twitter_api, mongo_client, **kwargs):
		self.track_settings=track_settings
		self.twitter_api=twitter_api
		self.mongo_client=mongo_client
		self.db_names=self.get_db_names(self.track_settings)
		self.track_list=self.get_track_list(self.track_settings)
		self.add_source_users(self.db_names, self.track_list, self.twitter_api, self.mongo_client, **kwargs)

	def get_db_names(self, track_settings):
		return [ track_obj['db_name'] for track_obj in track_settings ]

	def get_track_list(self, track_settings):
		return [ track_obj['track'] for track_obj in track_settings ] 


	def add_source_users(self, db_names, track_list, twitter_api, mongo_client, **kwargs):
		is_resume = False
		if len(kwargs) > 0:
			resume_db_name = kwargs.get("db_name")
			resume_track_item = kwargs.get("track_item")
			resume_pageNum = kwargs.get("pageNum")
			resume_counter = kwargs.get("counter")
			is_resume = True

		if is_resume:
			logging.warning("-"*10+"Resume: Adding Sources...")
			print "\n"+"-"*10+"Resume: Adding Sources..."
		else:
			logging.warning("-"*10+"Adding Sources...")
			print "\n"+"-"*10+"Adding Sources..."

		for index, db_name in enumerate(db_names):

			if is_resume:
				if db_name != resume_db_name:
					continue

			collection_handler = mongo_client[db_name]['SourceInfluencers']
			history_handler = mongo_client['History']['history_source']
			for track_item in track_list[index]:

				if is_resume:
					if track_item != resume_track_item:
						continue

				logging.warning("Adding Source: db_name: {0}: track_item: {1} ...".format( db_name, track_item ))
				print 			"\nAdding Source: db_name: {0}: track_item: {1} ...".format( db_name, track_item )
				
				if is_resume: 
					pageNum = resume_pageNum
				else:
					pageNum=1
				
				query_url=self.define_users_search_request_url_query(track_item.lower())
				users_search_request_url_full=query_url+"&page={0}&count=20".format(pageNum) 

				while True:
					while True:
						try:
							(header, data)=twitter_api.request(users_search_request_url_full)
							utils.safe_mode(header, "users/search")
							break
						except httplib2.ServerNotFoundError:
							logging.error("Unable to find api.twitter.com [add_source_users]. Sleeping for 30 seconds ...")
							print 		  "Unable to find api.twitter.com [add_source_users]. Sleeping for 30 seconds ..."
							time.sleep(30)	
						except SocketError as e:
							if e.errno == errno.ECONNRESET:
								logging.error("Connection reset by peer [add_source_users]. Sleeping for 30 seconds ...")
								print 		  "Connection reset by peer [add_source_users]. Sleeping for 30 seconds ..."
								time.sleep(30)
							else:
								raise e

					try:
						data=json.loads(data)
					except Exception as e:
						pass

					if header['status'] != '503':
						break
					else:
						logging.warning("Server Overload [add_source_users]. Sleeping for 30 seconds ...")
						print 			"Server Overload [add_source_users]. Sleeping for 30 seconds ..."
						time.sleep(30)


				while header['status'] == '200':
					for counter, each in enumerate(data):

						if is_resume:
							if counter != resume_counter:
								continue

						is_resume = False

						if collection_handler.find_one({'screen_name': each['screen_name']}) is None:
							print "db_name: {0}  track_item: {1}  pageNum: {2}  source_screen_name: {3}".format( db_name, track_item, pageNum, each['screen_name'] ) 
							collection_handler.update_one( {'screen_name': each['screen_name']}, {'$set': each}, upsert=True )
							history_handler.replace_one( {'operation': {'$exists': True} }, {'status':'incomplete', 'operation':'SourceCollector', 'db_name': db_name, 'track_item':track_item, 'pageNum': pageNum, 'counter':counter}, upsert=True )
						else:
							print "db_name: {0}  track_item: {1}  pageNum: {2}  source_screen_name: {3} User Exists".format( db_name, track_item, pageNum, each['screen_name'] ) 
							continue

					pageNum+=1
					users_search_request_url_full=query_url+"&page={0}&count=20".format(pageNum) 

					while True:
						while True:
							try:
								(header, data)=twitter_api.request(users_search_request_url_full)
								utils.safe_mode(header, "users/search")
								break
							except httplib2.ServerNotFoundError:
								logging.error("Unable to find api.twitter.com [add_source_users2]. Sleeping for 30 seconds ...")
								print 		  "Unable to find api.twitter.com [add_source_users2]. Sleeping for 30 seconds ..."
								time.sleep(30)
							except SocketError as e:
								if e.errno == errno.ECONNRESET:
									logging.error("Connection reset by peer [add_source_users2]. Sleeping for 30 seconds ...")
									print 		  "Connection reset by peer [add_source_users2]. Sleeping for 30 seconds ..."
									time.sleep(30)
								else:
									raise e

						try:
							data=json.loads(data)
						except Exception as e:
							pass

						if header['status'] != '503':
							break
						else:
							logging.warning("Server Overload [add_source_users2]. Sleeping for 30 seconds ...")
							print 			"Server Overload [add_source_users2]. Sleeping for 30 seconds ..."
							time.sleep(30)


				try:
					if header['status'] == '420': # this will not happen, but still
						raise apiErrors.RateLimited("Twitter API: Rate Limit Exceeded [users/search]", header['status'], data['errors'] if 'errors' in data else data)
					elif header['status'] == '403':
						raise apiErrors.Forbidden("Twitter API: Forbidden [users/search]", header['status'], data['errors'] if 'errors' in data else data)
					elif header['status'] == '401':
						raise apiErrors.Protected("Twitter API: Protected [users/search]", header['status'], data['errors'] if 'errors' in data else data)
					elif header['status'] == '404':
						raise apiErrors.UserNotFound("Twitter API: UserNotFound [users/search]", header['status'], data['errors'] if 'errors' in data else data)
					elif header['status'] == '400':
						raise apiErrors.PageLimited("Twitter API: No more pages [users/search]", header['status'], data['errors'] if 'errors' in data else data )
					else:
						raise Exception("Unknown Twitter API error: HTTP code: {0} Error Message: {1}".format(header['status'], str(data['errors']) if 'errors' in data else str(data) ))

				except apiErrors.RateLimited as e:
					logging.error(str(e))
					logging.error("Sleeping in unsafe mode: Rate Limited")
					print "Sleeping in unsafe mode: Rate Limited"
					time.sleep(15*60+10)
					pass
				except apiErrors.PageLimited as e:
					logging.warning("This is OK! "+str(e))
					logging.warning("Done with Adding Source: db_name: {0}: track_item: {1} ...".format( db_name, track_item ))
					print 			"Done with Adding Source: db_name: {0}: track_item: {1} ...".format( db_name, track_item )
					pass
				except apiErrors.Forbidden as e:
					logging.warning("This is OK! {0}".format(str(e)))
					pass
				except apiErrors.Protected as e:
					logging.warning("This is OK! {0}".format(str(e)))
					pass
				except apiErrors.UserNotFound as e:
					logging.warning("This is OK! {0}".format(str(e)))
					pass
				except Exception as e:
					pprint.pprint(header)
					print data
					logging.error(str(e))
					raise e

		history_handler.replace_one( {'operation': {'$exists': True} }, {'status':'complete', 'operation':'SourceCollector'}, upsert=True )
		logging.warning("Adding Sources: All Done!")
		print 			"\nAdding Sources: All Done!"
		return



	def define_users_search_request_url_query(self, track_item):
		base_url="https://api.twitter.com/1.1/users/search.json?q="
		predicates=track_item.split()
		if len(predicates)>1:
			return base_url+"%20".join(predicates) 
		else:
			return base_url+track_item





class FollowersCollector(object):
	def __init__(self, track_settings, twitter_api, mongo_client, **kwargs):
		self.track_settings=track_settings
		self.twitter_api=twitter_api
		self.mongo_client=mongo_client
		self.db_names=self.get_db_names(self.track_settings)
		self.add_followers_id(self.db_names, self.twitter_api, self.mongo_client, **kwargs)

	def get_db_names(self, track_settings):
		return [ track_obj['db_name'] for track_obj in track_settings ]


	def add_followers_id(self, db_names, twitter_api, mongo_client, **kwargs):
		is_resume = False
		if len(kwargs) > 0:
			resume_db_name = kwargs.get("db_name")
			resume_source_index = kwargs.get("source_index")
			resume_cursor_counter = kwargs.get("cursor_counter")
			resume_id_index = kwargs.get("id_index")
			is_resume = True

		if is_resume:
			logging.warning("-"*10+"Resume: Adding Followers ...")
			print 		"\n"+"-"*10+"Resume: Adding Followers ..."			
		else:
			logging.warning("-"*10+"Adding Followers ...")
			print 		"\n"+"-"*10+"Adding Followers ..."

		for db_name in db_names:

			if is_resume:
				if db_name != resume_db_name:
					continue

			followers_handler = mongo_client[db_name]['Followers']
			source_handler = mongo_client[db_name]['SourceInfluencers']
			history_handler = mongo_client['History']['history_followers']
			sources = source_handler.find()
			sources_count = source_handler.find().count()
			for source_index, source in enumerate(sources):

				if is_resume:
					if source_index != resume_source_index:
						continue

				logging.warning("Adding Followers of source: {0} in db_name: {1} Followers count: {2}".format(source['screen_name'], db_name, source['followers_count'] ))
				print 			"\nAdding Followers of source: {0} in db_name: {1} Followers count: {2}".format(source['screen_name'], db_name, source['followers_count'] )
				cursor_counter = 0
				cursor=-1
				followers_ids_url="https://api.twitter.com/1.1/followers/ids.json?cursor={0}&screen_name={1}&count=5000".format( cursor, source['screen_name'] )
				
				while True:
					while True:
						try:
							(header, data) = twitter_api.request(followers_ids_url)
							utils.safe_mode(header, "followers/ids")
							break
						except httplib2.ServerNotFoundError:
							logging.error("Unable to find api.twitter.com [followers/id]. Sleeping for 30 seconds ...")
							print 		  "Unable to find api.twitter.com [followers/id]. Sleeping for 30 seconds ..."
							time.sleep(30)
						except SocketError as e:
							if e.errno == errno.ECONNRESET:
								logging.error("Connection reset by peer [followers/id]. Sleeping for 30 seconds ...")
								print 		  "Connection reset by peer [followers/id]. Sleeping for 30 seconds ..."
								time.sleep(30)
							else:
								raise e

					try:
						data = json.loads(data)
					except Exception as e:
						pass

					if header['status'] != '503':
						cursor_counter+=1
						break
					else:
						logging.warning("Server Overload [followers/ids]. Sleeping for 30 seconds ...")
						print 			"Server Overload [followers/ids]. Sleeping for 30 seconds ..."
						time.sleep(30)

				while header['status'] == '200':
					cursor_length = len(data['ids'])
					for id_index, each_id in enumerate(data['ids']):

						if is_resume:
							if cursor_counter != resume_cursor_counter:
								continue
							else:
								if id_index != resume_id_index:
									continue

						is_resume = False

						if followers_handler.find_one({'id': each_id}) is None:

							users_show_url = "https://api.twitter.com/1.1/users/show.json?user_id={0}&include_entities=true".format(each_id)
							
							while True:
								while True:
									try:
										(header_2, data_2) = twitter_api.request(users_show_url)
										utils.safe_mode(header_2, "users/show")
										break
									except httplib2.ServerNotFoundError:
										logging.error("Unable to find api.twitter.com [users/show]. Sleeping for 30 seconds ...")
										print 		  "Unable to find api.twitter.com [users/show]. Sleeping for 30 seconds ..."
										time.sleep(30)
									except SocketError as e:
										if e.errno == errno.ECONNRESET:
											logging.error("Connection reset by peer [users/show]. Sleeping for 30 seconds ...")
											print 		  "Connection reset by peer [users/show]. Sleeping for 30 seconds ..."
											time.sleep(30)
										else:
											raise e

								try:
									data_2 = json.loads(data_2)
								except Exception as e:
									pass

								if header_2['status'] != '503':
									break
								else:
									logging.warning("Server Overload [users/show]. Sleeping for 30 seconds ...")
									print 			"Server Overload [users/show]. Sleeping for 30 seconds ..."
									time.sleep(30)


							if header_2['status'] == '200':
								print "db_name: {0}  source: {1}  src_index: {2}  flwrs_count: {3}".format(db_name, source['screen_name'], str(source_index)+'/'+str(sources_count), source['followers_count']) + " "*(9-len(str(source['followers_count']))) + "cursor: {0}  flwrs_id: {1}".format( cursor_counter, each_id ) + " "*(21-len(str(each_id))) + "cursor_position: {0}  flwrs_screen_name: {1}".format( str(id_index+1)+"/"+str(cursor_length), data_2['screen_name'] )
								followers_handler.update_one( {'id': each_id}, {'$set': data_2}, upsert=True )
								history_handler.replace_one( {'operation': {'$exists': True} }, {'status':'incomplete', 'operation':'FollowersCollector', 'db_name': db_name, 'source_index':source_index, 'cursor_counter': cursor_counter, 'id_index':id_index}, upsert=True )

							else:
								try:
									if header_2['status'] == '420':
										raise apiErrors.RateLimited("Twitter API: Rate Limit Exceeded [users/show]", header_2['status'], data_2['errors'] if 'errors' in data_2 else data_2 )
									elif header_2['status'] == '403':
										raise apiErrors.Forbidden("Twitter API: Forbidden [users/show]", header_2['status'], data_2['errors'] if 'errors' in data_2 else data_2)
									elif header_2['status'] == '404':
										raise apiErrors.UserNotFound("Twitter API: UserNotFound [users/show]", header_2['status'], data_2['errors'] if 'errors' in data_2 else data_2)
									elif header_2['status'] == '401':
										raise apiErrors.Protected("Twitter API: Protected [users/show]", header_2['status'], data_2['errors'] if 'errors' in data_2 else data_2)
									else:
										raise Exception("Unknown Twitter API error: HTTP code: {0} Error Message: {1}".format(header_2['status'], str(data_2['errors']) if 'errors' in data_2 else str(data_2) ))

								except apiErrors.RateLimited as e:
									logging.error(str(e))
									logging.error("Sleeping in unsafe mode: Rate Limited")
									print "Sleeping in unsafe mode: Rate Limited"
									time.sleep(15*60+10)
									pass
								except apiErrors.Forbidden as e:
									logging.warning("This is OK! {0}".format(str(e)))
									pass
								except apiErrors.Protected as e:
									logging.warning("This is OK! {0}".format(str(e)))
									pass
								except apiErrors.UserNotFound as e:
									logging.warning("This is OK! {0}".format(str(e)))
									pass
								except Exception as e:
									pprint.pprint(header_2)
									print data_2
									logging.error(str(e))
									raise e

						else:
							print "db_name: {0}  source: {1}  src_index: {2}  flwrs_count: {3}".format(db_name, source['screen_name'], str(source_index)+'/'+str(sources_count), source['followers_count']) + " "*(9-len(str(source['followers_count']))) + "cursor: {0}  flwrs_id: {1}".format( cursor_counter, each_id ) + " "*(21-len(str(each_id))) + "cursor_position: {0}  User Exists".format( str(id_index+1)+"/"+str(cursor_length) )
							continue

					if data['next_cursor'] !=0:
						next_cursor = data['next_cursor']
						followers_ids_url="https://api.twitter.com/1.1/followers/ids.json?cursor={0}&screen_name={1}&count=5000".format( next_cursor, source['screen_name'] )
						
						while True:
							while True:
								try:
									(header, data) = twitter_api.request(followers_ids_url)
									utils.safe_mode(header, "followers/ids")
									break
								except httplib2.ServerNotFoundError:
									logging.error("Unable to find api.twitter.com [followers/id2]. Sleeping for 30 seconds ...")
									print 		  "Unable to find api.twitter.com [followers/id2]. Sleeping for 30 seconds ..."
									time.sleep(30)
								except SocketError as e:
									if e.errno == errno.ECONNRESET:
										logging.error("Connection reset by peer [followers/id2]. Sleeping for 30 seconds ...")
										print 		  "Connection reset by peer [followers/id2]. Sleeping for 30 seconds ..."
										time.sleep(30)
									else:
										raise e
							
							try:
								data = json.loads(data)
							except Exception as e:
								pass

							if header['status'] != '503':
								cursor_counter+=1
								break
							else:
								logging.warning("Server Overload [followers/ids2]. Sleeping for 30 seconds ...")
								print 			"Server Overload [followers/ids2]. Sleeping for 30 seconds ..."
								time.sleep(30)

					else:
						break


				try:
					if header['status'] == '200':
						pass
					elif header['status'] == '420':
						raise apiErrors.RateLimited("Twitter API: Rate Limit Exceeded [followers/ids]", header['status'], data['errors'] if 'errors' in data else data)
					elif header['status'] == '403':
						raise apiErrors.Forbidden("Twitter API: Forbidden [followers/ids]", header['status'], data['errors'] if 'errors' in data else data)
					elif header['status'] == '404':
						raise apiErrors.UserNotFound("Twitter API: UserNotFound [followers/ids]", header['status'], data['errors'] if 'errors' in data else data)
					elif header['status'] == '401':
						raise apiErrors.Protected("Twitter API: Protected [followers/ids]", header['status'], data['errors'] if 'errors' in data else data)
					else:
						raise Exception("Unknown Twitter API error: HTTP code: {0} Error Message: {1}".format(header['status'], str(data['errors']) if 'errors' in data else str(data) ))

				except apiErrors.RateLimited as e:
					logging.error(str(e))
					logging.error("Sleeping in unsafe mode: Rate Limited")
					print "Sleeping in unsafe mode: Rate Limited"
					time.sleep(15*60+10)
					pass
				except apiErrors.Forbidden as e:
					logging.warning("This is OK! {0}".format(str(e)))
					pass
				except apiErrors.Protected as e:
					logging.warning("This is OK! {0}".format(str(e)))
					pass
				except apiErrors.UserNotFound as e:
					logging.warning("This is OK! {0}".format(str(e)))
					pass
				except Exception as e:
					pprint.pprint(header)
					print data
					logging.error(str(e))
					raise e

				logging.warning("Done with Adding Followers of source: {0} in db_name: {1}".format(source['screen_name'], db_name ))
				print 			"Done with Adding Followers of source: {0} in db_name: {1}".format(source['screen_name'], db_name )

		history_handler.replace_one( {'operation': {'$exists': True} }, {'status':'complete', 'operation':'FollowersCollector'}, upsert=True )
		logging.warning("Adding Followers: All Done!")
		print 			"\nAdding Followers: All Done!"
		return





if __name__ == "__main__":
	try:
		logging.warning("-"*10+"New Twitter API Session Opened")
		print "\n"+"-"*10+"New Twitter API Session Opened"
		utils.check_depleted_endpoints()

		mongodb_settings=settings.applications_settings.get("mongodb_settings")
		db=Database( mongodb_settings[0].get("db_ip"), mongodb_settings[0].get("db_port") )

		track_settings = settings.applications_settings.get("track_settings")
		auth_settings = settings.applications_settings.get("auth_settings")

		application=TwitterApplication(			
												auth_settings[0].get("consumer_key"), 
												auth_settings[0].get("consumer_secret"), 
												auth_settings[0].get("access_token"), 
												auth_settings[0].get("access_token_secret")
																								)

		flag_source = utils.is_complete_source("SourceCollector", db.client)

		if flag_source == 'first_run':
			source_collector=SourceCollector(track_settings, application.twitter_api, db.client)
		elif flag_source == 'complete':
			logging.warning("-"*10+"Resume: Skipping Operation Adding Sources")
			print 		"\n"+"-"*10+"Resume: Skipping Operation Adding Sources"
			pass
		else:
			source_collector=SourceCollector(track_settings, application.twitter_api, db.client, **flag_source)



		flag_followers = utils.is_complete_followers("FollowersCollector", db.client)

		if flag_followers == "first_run":
			followers_collector = FollowersCollector(track_settings, application.twitter_api, db.client)
		elif flag_followers == 'complete':
			logging.warning("-"*10+"Resume: Skipping Operation Adding Followers")
			print 		"\n"+"-"*10+"Resume: Skipping Operation Adding Followers"
			pass
		else:
			followers_collector = FollowersCollector(track_settings, application.twitter_api, db.client, **flag_followers)



		logging.warning("Finished. All Done.\n")
		print 			"\nFinished. All Done."

	except KeyboardInterrupt as e:
		logging.warning("Keyboard Interrupt\n")
		print str(e)
