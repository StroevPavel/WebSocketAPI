# coding: utf-8
import MySQLdb
from typing import Optional, Union, Any, Dict, List




class MyDB:
	def __init__(self):
		self.sql_details: Dict[str, Union[str, int]] = {}
		self.accounts_cache: Dict[str, Any] = {}


	def createINSERTsql(self, acc_data):
		# Все необходимые поля, кроме Id и Name - они и так есть всегда
		allFields = ['QuoterGroupId', 'ExecutorGroupId', 'Permissions', 'Code', 'UseFokOrders', 'PbCommission','SpotLimit','SpotDayLimit','ForwardLimit']
		sqlFields = []
		i = 0
		while i < len(allFields):
			if allFields[i] in acc_data:
				sqlFields.append(allFields[i])
			i += 1

		sql = "INSERT INTO accounts (AccID, Name"
		i = 0
		while i < len(sqlFields):
			sql = sql + ", " + sqlFields[i]
			i += 1
		sql = sql + ") VALUES (" + str(acc_data['Id']) + ", '" + acc_data['Name'] + "'"

		i = 0
		while i < len(sqlFields):
			if sqlFields[i] == 'Code':
				sql = sql + ", '" + acc_data[sqlFields[i]] + "'"
			elif (sqlFields[i] == 'SpotLimit') or (sqlFields[i] == 'SpotDayLimit') or (sqlFields[i] == 'ForwardLimit'):
				dct = acc_data[sqlFields[i]]
				dctToStr = ";".join(["%s=%s" % (k, v) for k, v in dct.items()])
				sql = sql + ", '" + dctToStr + "'"
			else:
				sql = sql + ", " + str(acc_data[sqlFields[i]])
			i += 1
		sql = sql + ")"

		return sql


	def start(self, cd: Dict[str, Union[str, int]]):
		try:
			cd = self.sql_details
			DBconn = MySQLdb.connect(host=cd['sql_ip'], user=cd['sql_login'], passwd=cd['sql_pass'], db=cd['sql_db'], use_unicode=True, charset="utf8")
			self.SqlConnect = DBconn 
			print("MySQL connect - OnLine")
		except MySQLdb.Error as err:
			self.err_handle(1, err)
			DBconn.close()


	def sendQuerry(self, querry: str):
		try:
			cursor = self.SqlConnect.cursor()
			print(querry)
			cursor.execute(querry)
			self.SqlConnect.commit()
		except MySQLdb.Error as err:
			self.err_handle(2, err)


	def clearTable(self, table: str):
		try:
			cursor = self.SqlConnect.cursor()
			sql = "DELETE FROM " + table + " WHERE id > 0"
			print(sql)
			cursor.execute(sql)
			self.SqlConnect.commit()
		except MySQLdb.Error as err:
			self.err_handle(2, err)


	def BackUpAccounts(self, acc_data):
		self.clearTable('accounts')
		for key in acc_data:
			sql = self.createINSERTsql(acc_data[key])
			print(sql)
			try:
				cursor = self.SqlConnect.cursor()
				cursor.execute(sql)
				self.SqlConnect.commit()
			except MySQLdb.Error as err:
				self.err_handle(2, err)
				

	def err_handle(self, err_num, param):
		if err_num == 1:
			print("Ошибка подключения к базе данных! Проверьте настройки подключения!")
			print("Connection error: {}".format(param))
		elif err_num == 2:
			print("Ошибка обращения к базе данных, проверьте соединение")
			print("Query error: {}".format(param))


	def logout(self):
		try:
			self.SqlConnect.close()
			print("MySQL disconnect - OffLine")
		except MySQLdb.Error as err:
			self.err_handle(2, err)


