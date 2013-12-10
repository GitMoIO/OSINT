import sys
import time
import os
import re
import simplejson
import urllib
import urllib2
import httplib
import json
import logging
from pymongo import *

MAX_LIMIT=50
SLEEP_TIME=2
DB_NAME="facebook"


#-----------------------------------------------------------------------------

def userExists(db, idUser):
	try:
		count=db.users.find({"id":str(idUser)}).count()
		return count
	except Exception:
		import traceback
		logger.error('generic exception: ' + traceback.format_exc())


#-----------------------------------------------------------------------------

def insertUser(db, idUser, response):
	try:
		d=json.loads(response)
		db.users.update({"id":str(idUser)}, {"$set":d}, upsert=True)
	except Exception:
		import traceback
		logger.error('generic exception: ' + traceback.format_exc())
		logger.error("user "+str(iduser)+" not processed")


##############################################################################
##############################################################################



# set up logging to file
logging.basicConfig(level=logging.DEBUG,
		format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
		datefmt='%Y-%m-%d %H:%M',
		filename='facebook_LOG.log',
		filemode='w')

# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)

# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s %(name)-12s: %(levelname)-8s %(message)s', '%Y-%m-%d %H:%M')

# tell the handler to use this format
console.setFormatter(formatter)

# add the handler to the root logger
logging.getLogger('').addHandler(console)

logger = logging.getLogger("main")


url="http://graph.facebook.com"

#connecto to db
try:
	con=Connection()
	db=con[DB_NAME]
except:
	import traceback
	logger.error('generic exception: ' + traceback.format_exc())
	logger.error("Error connecting to DB")
	sys.exit(1)

for i in range(4, MAX_LIMIT):
	urlP=url+"/"+str(i)

	req = urllib2.Request(urlP)

	try:
		exists=userExists(db, i)
		if exists > 0:
			print "user "+str(i)+" exists!"
			continue

		response=urllib2.urlopen(req)
		jsonResponse=response.read()
		print jsonResponse

		if not jsonResponse:
			logger.error("user "+i+" not processed")
		else:
			insertUser(db, i, jsonResponse)

		time.sleep(SLEEP_TIME)

	except urllib2.HTTPError, e:
		logger.error('HTTPError = ' + str(e.code))
		logger.error("user "+str(i)+" not processed")
		time.sleep(SLEEP_TIME)
		continue

	except urllib2.URLError, e:
		logger.error('URLError = ' + str(e.reason))
		logger.error("user "+str(i)+" not processed")
		time.sleep(SLEEP_TIME)
		continue

	except httplib.HTTPException, e:
		logger.error('HTTPException')
		logger.error("user "+str(i)+" not processed")
		time.sleep(SLEEP_TIME)
		continue

	except Exception:
		import traceback
		logger.error('generic exception: ' + traceback.format_exc())
		logger.error("user "+str(i)+" not processed")
		time.sleep(SLEEP_TIME)
		continue
