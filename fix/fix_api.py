# coding: utf-8
import socket
import datetime
import sys
import asyncio
from typing import Optional, AsyncIterable, cast, Union, Any, Dict, Iterable, List

from fix.fix_msg_types import FIXRequestType


class MDFix:
	def __init__(self):
		self.MD_details: Dict[str, Union[str, int]] = {}
		self.MD_socket = None
		self.request_id: int = 1
		self.send_queue: List[Any] = []  # очередь сообщений на отправку
		self.quotes: Dict[str, Union[str, int]] = {}


	async def start(self):
		mdSession = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.MD_socket = mdSession
		cd = self.MD_details

		with mdSession:
			try:
				mdSession.connect((cd['HOST'], cd['PORT']))
				self.send_queue.append(self.get_message(FIXRequestType.Logon,{}))

				done, pending = await asyncio.wait([self.receiveFromSocket(), self.sendToSocket()], return_when=asyncio.FIRST_COMPLETED)

				for future in pending:
					future.cancel()
			except socket.error as err:
				print(err)
				print("Error! Need to reconnect MD session")


	def get_message(self, request_type: FIXRequestType, data: Dict = None):
		if data is None:
			data = {}

		msgHead = '8=FIX.4.4|9='
		base_part = '35=' + request_type + '|49=' + self.MD_details['SenderCompID'] + '|56=' + self.MD_details['TargetCompID'] + '|34=' + str(self.request_id) + '|52=' + str(self.current_datetime()) + '|'
		
		if request_type == FIXRequestType.Logon:
			msgBody = [base_part + '98=0|108=30|']
		
		if (request_type == FIXRequestType.Heartbeat):
			msgBody = [base_part]

		if (request_type == FIXRequestType.MarketDataRequest):
			msgBody = [base_part + '262=' + str(data['instrument']) + '|263=1|264=0|265=0|146=1|55=' + str(data['instrument']) + '|267=2|269=0|269=1|']

		msgHead = msgHead + str(len(msgBody[0])) 
		FIXmsg = msgHead + '|' + msgBody[0]
		FIXmsg = FIXmsg.replace('|', '\x01')
		FIXmsg = FIXmsg + '10=' + self.getCKsum(FIXmsg) + '\x01'

		request_id = self.request_id
		self.request_id += 1

		return FIXmsg


	async def receiveFromSocket(self):
		while True:
			await asyncio.sleep(0.1)
			responce = self.MD_socket.recv(1024)
			if responce:
				message = await self.parseMsg(responce.decode('ascii'))


	async def sendToSocket(self):
		while True:
			if self.send_queue:
				message = self.send_queue.pop(0)
				try:
					print('-->' + message)
					self.MD_socket.send(message.encode('ascii'))
				except OSError as msg:
					print(msg)
					return
			else:
				await asyncio.sleep(0.1)


	async def parseMsg(self, data):
		#print('<--' + data)

		# часто в полученном массиве данных склеиваются несколько соощений, разделим их на отдельные и обработаем по очереди
		msgs = data.split('8=FIX.4.4\x01')
		for onemsg in msgs:
			if len(onemsg) > 0:
				splMsg = onemsg.split('\x01')
				if '35=h' in splMsg: # Trading session status
					print('[Get msg 35=h (Trading session status)]' + onemsg)
					self.send_queue.append(self.get_message(FIXRequestType.MarketDataRequest,{'instrument': 'USD/RUB_TOM'}))
				elif '35=2' in splMsg:
					print('[Get msg 35=2 (Resend request)]' + onemsg)
					#sendToSocket('35=2', '', connID)
				elif '35=0' in splMsg:
					print('[Get msg 35=0 (Heartbeat)]' + onemsg)
					self.send_queue.append(self.get_message(FIXRequestType.Heartbeat,{}))
				elif '35=W' in splMsg:
					print('[Get msg 35=W (Market Data incremental refresh)]')
					self.marketDataUpdate(splMsg)
				else:
					print('[Get unknown message type!]' + onemsg)


	def current_datetime(self):
		return datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]


	def getCKsum(self, msg):
		cksum = str(sum([ord(i) for i in list(msg)]) % 256)
		while len(cksum) < 3:
			cksum = '0' + cksum
		return cksum


	def marketDataUpdate(self, msg):
		i = 0
		quote = {
			'instrument': '',
			'datetime': '',
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
				quote['band'] = float(fixList[i][1])
			if fixList[i][0] == '52':
				quote['datetime'] = fixList[i][1]
			if (fixList[i][0] == '269') and (fixList[i][1] == '0'):
				waitside = 'bid'
			if (fixList[i][0] == '269') and (fixList[i][1] == '1'):
				waitside = 'offer'
			if fixList[i][0] == '270':
				quote[waitside] = float(fixList[i][1])
			i += 1

		if (quote['bid'] > 0) and (quote['bid'] > 0):
			self.quotes = quote
			print(quote)