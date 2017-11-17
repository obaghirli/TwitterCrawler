import os
import tornado.auth
import tornado.ioloop
import tornado.web
import tornado.httpserver
import json
import sys



public_path = os.path.join(os.path.dirname(__file__), 'public')
template_path = os.path.join(os.path.dirname(__file__), 'template')


class JSONDatabase(object):
	def __init__(self):
		self.data={"Users":[]}

	def load_db(self):
		try:
			with open("users_db.json",'r') as database:
				self.data=json.load(database)
		except:
			with open('users_db.json', 'w') as database:
				database.write(json.dumps(self.data, separators=(',',':'), indent=4))

	def update(self):
		with open('users_db.json', 'w') as database:
			database.write(json.dumps(self.data, separators=(',',':'), indent=4))
 
	def get_user_init(self, username):
		for user in self.data["Users"]:
			if user["plain_auth_pair"].split(":")[0]==username:
				return user["firstname"][0].upper()+user["lastname"][0].upper()
		return ""



class BaseHandler(tornado.web.RequestHandler):
	def get_current_user(self):
		return self.get_secure_cookie("user")


class MainHandler(BaseHandler):

	def initialize(self):
		pass

	def prepare(self):
		pass

	@tornado.web.authenticated
	def get(self):
		global users_db
		username = self.current_user
		userinitials=users_db.get_user_init(username)
		self.render("user_profile.html",username=username, userinitials=userinitials )

	def on_finish(self):
		pass


class LoginHandler(BaseHandler):
	def get(self):
		self.render("index.html", register_error_message="", login_error_message="")


	def authenticate(self, plain_auth_pair):
		global users_db
		for user in users_db.data["Users"]:
			if user["plain_auth_pair"]==plain_auth_pair:
				return True
		return False

	def post(self):
		plain_auth_pair=self.request.arguments["plain_auth_pair"][0]
		if self.authenticate(plain_auth_pair):
			self.set_secure_cookie("user", plain_auth_pair.split(":")[0])
			self.redirect("/")
		else:
			self.render("index.html", register_error_message="", login_error_message="Invalid input")


class LogoutHandler(BaseHandler):
	def get(self):
		self.clear_cookie("user")
		self.redirect("/")


class RegisterHandler(BaseHandler):
	def exists(self, username):
		global users_db
		for user in users_db.data["Users"]:
			if user["plain_auth_pair"].split(":")[0]==username:
				return True
		return False

	def post(self):
		global users_db
		register_error_message=""
		plain_register_sequence=self.request.arguments["plain_register_sequence"][0]
		[firstname, lastname, email, username, password]=plain_register_sequence.split(":")
		new_user={ "firstname":firstname,"lastname":lastname,"email":email,"plain_auth_pair":username+":"+password }

		if self.exists(username):
			register_error_message="Username already exists."
			self.render("index.html", register_error_message=register_error_message, login_error_message="")
		else:
			users_db.data["Users"].append(new_user)
			self.set_secure_cookie("user", username)
			self.redirect("/")

	def on_finish(self):
		global users_db
		users_db.update()



class Application(tornado.web.Application):
	def __init__(self):
		handlers = [
		  (r'/', MainHandler),
		  (r'/register', RegisterHandler),
		  (r'/login', LoginHandler),
		  (r'/logout', LogoutHandler),
		  (r'/public/(.*)', tornado.web.StaticFileHandler, {'path': public_path})
		]		

		settings = dict(
		  debug=True,
		  template_path=template_path,
		  login_url="/login",
		  cookie_secret="ec5caa0594aa10199c4232c2ee8221d3c870ae6f"
		)

		tornado.web.Application.__init__(self, handlers, **settings)



if __name__=="__main__":
	users_db=JSONDatabase()
	users_db.load_db()
	http_server = tornado.httpserver.HTTPServer(Application())
	http_server.listen(8888)
	tornado.ioloop.IOLoop.instance().start()