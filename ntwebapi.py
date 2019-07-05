# coding: utf-8

import asyncio
from websocket import create_connection
import json
import time
import datetime

import positions


def getUnixDT():
	dt = datetime.datetime.utcnow()
	dts = str(time.mktime(dt.timetuple()) * 1000)
	return dts[:dts.find('.')]


def collectMsgParams(key):
	msgParams = {
	'Hello': {
		'VersionMajor': 2,
		'VersionMinor': 3
		},
	'SelectUserRoleRequest': {
		'SelectedRole': 2
		}
	}
	return msgParams[key]


def ntconnect(serv):
	if serv == 'uat':
		global ws
		global RequestId
		RequestId = 1
		ws = create_connection('wss://public-api-uat.ntprog.com:443')
		sendMsg('Hello','')

	elif serv == 'prod':
		pass

	else: 
		print("Check parameters (must be uat or prod")

async def sendPing():
	while True:
		await asyncio.sleep(3)
		makePingMsg()


def makePingMsg():
	json_msg = {
			'Type': 1,
			'Msg': getUnixDT()
		}
	msg_tosend = json.dumps(json_msg)
	print(">>! " + msg_tosend)
	global ws
	ws.send(msg_tosend)	


def login(clName, clPass):
	msgParams = {
		'UserName': clName,
		'Password': clPass,
		'ClientAppType': 1
		}
	sendMsg('LoginRequest', msgParams)
	sendMsg('SelectUserRoleRequest', '')


def logout():
	pass


def sendMsg(type, msg):
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

	json_msg = {
		'Type': 3,
		'Msg': data_json
		}
	msg_tosend = json.dumps(json_msg)
	sendToSocket(msg_tosend)


def sendToSocket(msg):
	global RequestId
	RequestId += 1
	print(">>! " + msg)
	global ws
	ws.send(msg)


async def receiveFromSocket(SubsAccPositions, SubsAcc): 
	while True:
		global ws
		msg = ws.recv()
		# print("<<! " + msg)
		hdDict = json.loads(msg)

		if (hdDict['Type'] == 1):
			print('<<! Get ping request: ' + msg)
			makePingMsg()

		elif (hdDict['Type'] == 2):
			print('<<= Get ping responce: ' + msg)

		elif (hdDict['Type'] == 3):
			positions.dataParser(json.loads(hdDict['Msg']),SubsAccPositions,SubsAcc)

		else:
			print("<<= " + msg)

		await asyncio.sleep(0.1)