# coding: utf-8

# netstat -ao					список процессов + порты
# tasklist | fined "4900"		данные о процессе по его PID

import socket
import datetime
import sys



mdUAT = {
	'HOST': '127.0.0.1', # 'HOST': 'uat-gate.ntprog.com' - настройки в stunnel.conf
	'PORT': 21011,
	'SenderCompID': 'CODE00_00_MD',
	'TargetCompID': 'NTPROUAT'
}



def current_datetime():
	return datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]



def getCKsum(msg):
	cksum = str(sum([ord(i) for i in list(msg)]) % 256)
	while len(cksum) < 3:
		cksum = '0' + cksum
	return cksum



def marketDataUpdate(msg):
	i = 0
	quote = {
		'instrument': '',
		'band': 0,
		'bid': 0,
		'offer': 0
	}
	fixList = []
	while i < len(msg) - 1:
		tag = msg[i].split('=')
		fixList.append(tag)
		i += 1
	i = 0
	while i < len(fixList):
		if fixList[i][0] == '55':
			quote['instrument'] = fixList[i][1]
		if fixList[i][0] == '271':
			quote['band'] = fixList[i][1]
		if (fixList[i][0] == '269') and (fixList[i][1] == '0'):
			waitside = 'bid'
		if (fixList[i][0] == '269') and (fixList[i][1] == '1'):
			waitside = 'offer'
		if fixList[i][0] == '270':
			quote[waitside] = fixList[i][1]
		i += 1
	print(quote)



def createMsg(msgType, msgParam):
	msgHead = '8=FIX.4.4|9='
	global SeqNum
	if (msgType == 'logon'):
		msgBody = ['35=A|49=' + mdUAT.get('SenderCompID') + '|56=' + mdUAT.get('TargetCompID') + '|34=' + str(SeqNum) + '|52=' + str(current_datetime()) + '|98=0|108=30|']
		msgHead = msgHead + str(len(msgBody[0])) 
		FIXmsg = msgHead + '|' + msgBody[0]
		FIXmsg = FIXmsg.replace('|', '\x01')
		FIXmsg = FIXmsg + '10=' + getCKsum(FIXmsg) + '\x01'

	if (msgType == 'subscribe'):
		msgBody = ['35=V|49=' + mdUAT.get('SenderCompID') + '|56=' + mdUAT.get('TargetCompID') + '|34=' + str(SeqNum) + '|52=' + str(current_datetime()) + '|262=' + msgParam + '|263=1|264=0|265=0|146=1|55=' + msgParam + '|267=2|269=0|269=1|']
		msgHead = msgHead + str(len(msgBody[0])) 
		FIXmsg = msgHead + '|' + msgBody[0]
		FIXmsg = FIXmsg.replace('|', '\x01')
		FIXmsg = FIXmsg + '10=' + getCKsum(FIXmsg) + '\x01'

	if (msgType == 'heartbeat'):
		msgBody = ['35=0|49=' + mdUAT.get('SenderCompID') + '|56=' + mdUAT.get('TargetCompID') + '|34=' + str(SeqNum) + '|52=' + str(current_datetime()) + '|']
		msgHead = msgHead + str(len(msgBody[0])) 
		FIXmsg = msgHead + '|' + msgBody[0]
		FIXmsg = FIXmsg.replace('|', '\x01')
		FIXmsg = FIXmsg + '10=' + getCKsum(FIXmsg) + '\x01'

	print('--> ' + FIXmsg)
	global lastMSG 
	lastMSG = FIXmsg
	return FIXmsg



def subscribeToInstruments(msg, connID):
	if msg == 'All':
		sendToSocket('35=V', 'USD/RUB_TOM',connID)



def parseMsg(msg, connID):
	print('<-- ' + msg)
	msgs = msg.split('8=FIX.4.4\x01')
	for onemsg in msgs:
		if len(onemsg) > 0:
			splMsg = onemsg.split('\x01')
			if '35=h' in splMsg: # Trading session status
				print('[Get msg 35=h (Trading session status)]')
				subscribeToInstruments('All', connID)
			elif '35=2' in splMsg:
				print('[Get msg 35=2 (Resend request)]')
				sendToSocket('35=2', '', connID)
			elif '35=0' in splMsg:
				print('[Get msg 35=0 (Heartbeat)]')
				sendToSocket('35=0', '', connID)
			elif '35=W' in splMsg:
				print('[Get msg 35=W (Market Data incremental refresh)]')
				marketDataUpdate(splMsg)

	receiveFromSocket(connID)



def sendToSocket(msg, msgParam, connID):
	global SeqNum
	SeqNum += 1
	if (msg == '35=V'):
		connID.send(str(createMsg('subscribe', msgParam)).encode('ascii'))
	if (msg == '35=2'):
		global lastMSG
		connID.send(str(lastMSG).encode('ascii'))
		print('--> ' + lastMSG)
	if (msg == '35=0'):
		connID.send(str(createMsg('heartbeat', msgParam)).encode('ascii'))



def receiveFromSocket(connID): 
	while True:
		responce = connID.recv(1024)
		if responce:
			# parseMsg(responce.decode('ascii'), connID)
			break
	parseMsg(responce.decode('ascii'), connID)



def startApp(): 
	mdSession = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	global SeqNum
	SeqNum = 1
	with mdSession:
		try:
			mdSession.connect((mdUAT.get('HOST'), mdUAT.get('PORT')))
			mdSession.send(str(createMsg('logon', '')).encode('ascii'))
			receiveFromSocket(mdSession)
		except socket.error as err:
			print(err)
			print("Error")



startApp()