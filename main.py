# coding: utf-8

import asyncio
import ntwebapi

async def startApp(): 
	ntwebapi.ntconnect('uat')
	ntwebapi.login('StroevAdmin@ALFN3', 'Fx1234')
	for accId in SubsAcc:
		ntwebapi.sendMsg('SubscribePositions', accId)

SubsAcc = [53,57]

ioloop = asyncio.get_event_loop()
tasks = [ioloop.create_task(startApp()), ioloop.create_task(ntwebapi.receiveFromSocket()), ioloop.create_task(ntwebapi.sendPing())]
wait_tasks = asyncio.wait(tasks)
ioloop.run_until_complete(wait_tasks)
ioloop.close()
