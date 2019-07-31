# coding: utf-8

from websocket import create_connection
import json
import time
import datetime
import asyncio
import db_oper as db




sql_settings = {
	"sql_ip":"46.228.199.76",
	"sql_login":"volmdapp",
	"sql_pass":"Fx1234#",
	"sql_db":"volatility"
}



def save_acc_to_db(acc_data):
	global sql_conn
	if not db.save_acc(sql_conn, acc_data):
		sql_conn = db.db_set_connect(sql_settings["sql_ip"], sql_settings["sql_login"], sql_settings["sql_pass"], sql_settings["sql_db"])
		save_acc(sql_conn, acc_data)




def getUnixDT():
	#dt = datetime.datetime.utcnow()
	dt = datetime.datetime.now()
	dts = str(time.mktime(dt.timetuple()) * 1000)
	return dts[:dts.find('.')]



def makePingMsg(side):
	json_msg = {
			'Type': side,
			'Msg': getUnixDT()
		}
	msg_tosend = json.dumps(json_msg)
	WSsend(msg_tosend, side)



def collectMsgParams(key):
	msgParams = {
	'Hello': {
		'VersionMajor': 2,
		'VersionMinor': 4
		},
	'SelectUserRoleRequest': {
		'SelectedRole': 2
		}
	}
	return msgParams[key]



def makeDataMsg(type, msg):
	if type == 'Hello' or  type == 'SelectUserRoleRequest':
		data_json = json.dumps({
			'RequestId': RequestId,
			'OriginalRequestId': RequestId,
			'RequestType': type,
			'Message': collectMsgParams(type)
			})
	elif type == 'LoginRequest':
		data_json = json.dumps({
			'RequestId': RequestId,
			'OriginalRequestId': RequestId,
			'RequestType': type,
			'Message': msg
			})
	elif type == 'SubscribePositions':
		data_json = json.dumps({
			'RequestId': RequestId,
			'OriginalRequestId': RequestId,
			'RequestType': type,
			'Message': {
				'AccountId': msg
				}
			})
	elif type == 'RequestAccountStates':
		data_json = json.dumps({
			'RequestId': RequestId,
			'OriginalRequestId': RequestId,
			'RequestType': type,
			'Message': ''
			})

	json_msg = {
		'Type': 3,
		'Msg': data_json
		}
	msg_tosend = json.dumps(json_msg)
	WSsend(msg_tosend, 3)



def WSconnect():
	global RequestId
	RequestId = 1
	try:
		global ws_conn
		ws_conn = create_connection('wss://public-api-uat.ntprog.com:443')
		makeDataMsg('Hello','')
		#login('StroevAdmin@ALFN3', 'Fx1234')

		#makeDataMsg('RequestAccountStates','')

	except ws.Error as err:
		print(err)
		WSclose()



def parseMsg(msg):
	#print('<<<<!!!' + msg['RequestType'])
	assList = []
	if (msg['RequestType'] == 'Environment'):
		login('StroevAdmin@ALFN3', 'Fx1234')

	elif (msg['RequestType'] == 'LoginReply'):
		makeDataMsg('RequestAccountStates','')

	elif (msg['RequestType'] == 'AccountStatesUpdate'):
		AccStates = msg['Message']['AccountStates']
		i = 0
		global sql_conn
		db.clear_acc(sql_conn)
		while i < len(AccStates):
			save_acc_to_db(AccStates[i]['key'])
			i += 1
	else:
		print(msg)



async def WSrecieve():
	global ws_conn
	while True:
		await asyncio.sleep(0.1)
		msg = ws_conn.recv()
		hdDict = json.loads(msg)
		#print(hdDict)
		if (hdDict['Type'] == 1):
			print('<<..Get ping request..<< ' + msg)
			makePingMsg(2)

		elif (hdDict['Type'] == 2):
			print('<<..Get ping responce..<<' + msg)

		elif (hdDict['Type'] == 3):
			print('<<..Get data message..<<')
			parseMsg(json.loads(hdDict['Msg']))
		else:
			print('<<..Get unknown message..<<' + msg)



def WSsend(msg, msg_type):
	if msg_type == 1:
		print(">>..Send ping request..>> " + msg)
	elif msg_type == 2:
		print(">>..Send ping responce..>> " + msg)
	elif msg_type == 3:
		global RequestId
		RequestId += 1
		print(">>..Send data message..>> " + msg)
	else:
		print(">>..Send unknown message type..>> " + msg)
	print('>>>> ' + msg)
	global ws_conn
	ws_conn.send(msg)



def WSclose():
	global ws_conn
	ws_conn.close()



def login(clName, clPass):
	msgParams = {
		'UserName': clName,
		'Password': clPass,
		'ClientAppType': 1
		}
	makeDataMsg('LoginRequest', msgParams)
	makeDataMsg('SelectUserRoleRequest', '')



"""
def UpdateAccountStates(AccList):
	n = 0
	while n < len(AccList):
		#print(type(AccList[n]))
		global sql_conn
		save_acc_to_db(sql_conn, AccList[n])
		i += 1
"""


async def sendPing():
	while True:
		await asyncio.sleep(9)
		makePingMsg(1)



async def startApp(): 
	global sql_conn
	sql_conn = db.db_set_connect(sql_settings["sql_ip"], sql_settings["sql_login"], sql_settings["sql_pass"], sql_settings["sql_db"])
	WSconnect()




ioloop = asyncio.get_event_loop()
tasks = [
	ioloop.create_task(startApp()),
	ioloop.create_task(WSrecieve()),
	ioloop.create_task(sendPing()),
]
wait_tasks = asyncio.wait(tasks)
ioloop.run_until_complete(wait_tasks)
ioloop.close()