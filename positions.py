# coding: utf-8

def dataParser(msgDict, SubsAccPositions, SubsAcc):
	if (msgDict['RequestType'] == 'PositionSnapshot'):
		SubsAccPositions[msgDict['Message']['AccountId']] = msgDict['Message']['SpotOpenPosition']
		TotalPos = 0
		for accId in SubsAcc:
			if accId in SubsAccPositions:
				TotalPos = TotalPos + SubsAccPositions[accId]
		# TotalPos = TotalPos * -1
		print(SubsAccPositions)
		print(" TOTAL: " + str(TotalPos))

