applications_settings={

		"auth_settings":
							[

								{	
									"consumer_key": "", #App ID
									"consumer_secret": "", #App secret 
									"access_token": "", #Access token
									"access_token_secret": "" #Access token secret
								}


							],



		"track_settings":
							[


								{	
									"track": ['arduino'], 
									"db_name": "Arduino"

								},

								{	
									"track": ['raspberry pi'], 
									"db_name": "RaspberryPi"

								},
								{	
									"track": ['3d printer', '3d print'], 
									"db_name": "3DPrinter"

								}


							],


		"mongodb_settings":
							[

								{	
									"db_ip": "localhost", 
									"db_port": 27017

								}
			
							]
								

}