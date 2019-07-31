from enum import Enum

class FIXRequestType(str, Enum):
	# TAG 35
	Logon = 'A'
	Heartbeat = '0'
	TestRequest = '1'
	ResendRequest = '2'
	SessionReject = '3'
	SequenceReset = '4'
	Logout = '5'
	TradingSessionStatus = 'h'
	BusinessReject = 'j'
	MarketDataRequest = 'V'

