# coding: utf-8

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



def save_quote(sql_conn, quote):
	if (quote['bid'] > 0) and (quote['offer'] > 0):
		try:
			cursor = sql_conn.cursor()
			mid = (quote['bid'] + quote['offer']) / 2
			sql = "INSERT INTO quotes (instrument, date_time, date_time_str, band, bid, offer, mid) VALUES ('" + quote['instrument'] + "','" + format_date_time(quote['datetime']) + "', '" + quote['datetime'] + "', " + str(quote['band']) + ", " + str(quote['bid']) + ", " + str(quote['offer']) + ", " + str(mid) + ")"
			# print(sql)
			cursor.execute(sql)
			sql_conn.commit()
			return True
		except MySQLdb.Error as err:
			err_handle(2, err)
			return False



def clear_old_quotes(sql_conn):
	# удалить из quotes старые котировки
	try:
		cursor = sql_conn.cursor()
		sql = "DELETE FROM quotes WHERE date_time < ADDDATE(NOW(), INTERVAL -4 HOUR)"
		cursor.execute(sql)
		sql_conn.commit()
		print('DB Quotes cleaning completed!')
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



def format_date_time(dt_str):
	dtparts = dt_str.split('-')
	dy = dtparts[0][:4]
	dm = dtparts[0][4:6]
	dd = dtparts[0][6:]
	fdt= dy + '-' + dm + '-' + dd + ' ' + dtparts[1]
	return fdt