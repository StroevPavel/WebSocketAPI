# coding: utf-8
# dct = eval(str)

import MySQLdb




def db_set_connect(sql_ip, sql_login, sql_pass, sql_db):
	try:
		conn = MySQLdb.connect(host=sql_ip, user=sql_login, passwd=sql_pass, db=sql_db, use_unicode=True, charset="utf8")
		print("Переподключение к BD MySQL - ОК") # отладочное, удалить
		return conn
	except MySQLdb.Error as err:
		err_handle(1, err)
		# print("Connection error: {}".format(err))
		conn.close()




def createINSERTsql(acc_data):
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



def save_acc(sql_conn, acc_data):
	if (acc_data['Id'] > 0) :
		try:
			cursor = sql_conn.cursor()
			# sql = "INSERT INTO accounts (AccID, Name, QuoterGroupId, ExecutorGroupId, Permissions, Code, UseFokOrders, PbCommission) VALUES (" + str(acc_data['Id']) + ", '" + acc_data['Name'] + "'," + str(acc_data['QuoterGroupId']) + ", " + str(acc_data['ExecutorGroupId']) + ", " + str(acc_data['Permissions']) + ", '" + acc_data['Code'] + "', " + str(acc_data['UseFokOrders']) + ", " + str(acc_data['PbCommission']) + ")"
			sql = createINSERTsql(acc_data)
			print(sql)
			cursor.execute(sql)
			sql_conn.commit()
			return True
		except MySQLdb.Error as err:
			err_handle(2, err)
			return False



def clear_acc(sql_conn):
	try:
		cursor = sql_conn.cursor()
		sql = "DELETE FROM accounts WHERE id>0"
		cursor.execute(sql)
		sql_conn.commit()
		print("Table accounts is clear!")
		return True
	except MySQLdb.Error as err:
		err_handle(2, err)
		return False



def err_handle(err_num, param):
	if err_num == 1:
		# Ошибка подключения к базе данных! Проверьте настройки подключения!
		print("Ошибка подключения к базе данных! Проверьте настройки подключения!")
		print("Connection error: {}".format(param))
	elif err_num == 2:
		# Ошибка обращения к базе данных, проверьте соединение
		print("Ошибка обращения к базе данных, проверьте соединение")
		print("Query error: {}".format(param))