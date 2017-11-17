class APIErrors(Exception):
	def __init__(self, message=None, code=None, details=None ):
		self.message=message
		self.code=code
		self.details=details

	def __str__(self):
		return repr("Error message: {0} Error code: {1} Details: {2}".format( self.message, str(self.code), str(self.details) ) ) 

class AuthError(APIErrors):
	pass

class RateLimited(APIErrors):
	pass

class PageLimited(APIErrors):
	pass

class Forbidden(APIErrors):
	pass

class Protected(APIErrors):
	pass

class UserNotFound(APIErrors):
	pass

class Unknown(APIErrors):
	pass

