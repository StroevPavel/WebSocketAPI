import asyncio
import json
from logging import getLogger
from typing import Optional, AsyncIterable, cast, Union, Any, Dict, Iterable, List
from dataclasses import dataclass, field
from enum import Enum

import websockets

from ..constants import PUBLIC_WS_API_MAJOR_VERSION, PUBLIC_WS_API_MINOR_VERSION, NTPRO_USER, NTPRO_PASSWORD, NTPRO_WS_URL


class NTProMessageType(int, Enum):
    PING_REQUEST = 1
    PING_REPLY = 2
    DATA_MESSAGE = 3


class NTProRequestType(str, Enum):
    Hello = 'Hello'
    Environment = 'Environment'
    LoginRequest = 'LoginRequest'
    LoginReply = 'LoginReply'
    SelectUserRoleRequest = 'SelectUserRoleRequest'
    CommandReply = 'CommandReply'
    RequestAccountStates = 'RequestAccountStates'
    AccountStatesUpdate = 'AccountStatesUpdate'
    ChangeAccountRequest = 'ChangeAccountRequest'


@dataclass
class NTProMessage:
    Type: NTProMessageType
    Msg: str

    def to_json(self):
        return {
            'Type': self.Type,
            'Msg': self.Msg,
        }

    @staticmethod
    def from_json(json_data: Dict):
        json_data['Type'] = NTProMessageType(json_data['Type'])

        if json_data['Type'] == NTProMessageType.DATA_MESSAGE:
            json_data.update(json.loads(json_data['Msg']))
            json_data['Msg'] = json_data['Message']
            del json_data['Message']

            return NTProDataMessage(**json_data)

        return NTProMessage(**json_data)


@dataclass
class NTProDataMessage:
    RequestId: int
    OriginalRequestId: int
    RequestType: NTProRequestType

    Msg: dict = field(default_factory=dict)
    Type: NTProMessageType = NTProMessageType.DATA_MESSAGE

    def to_json(self):
        msg = {
            "RequestId": self.RequestId,
            "OriginalRequestId": self.OriginalRequestId,
            "RequestType": self.RequestType,
            "Message": self.Msg
        }

        return {
            'Type': self.Type,
            'Msg': json.dumps(msg),
        }



class WSApi:
    def __init__(self):
        self.logger = getLogger(__name__)

        self.websocket: Optional[websockets.WebSocketClientProtocol] = None  # здесь будет храниться объект с ws

        self.producer_queue: List[Union[NTProMessage, NTProDataMessage]] = []  # очередь сообщений на отправку
        self.sent_messages: Dict[int, NTProDataMessage] = {}  # запоминаем что отправили по request_id

        # чтобы формировать запросы на изменение аккаунтов нужно их сначала получить и записать
        self.accounts_cache: Dict[int, Dict[str, Any]] = {}

        # id запроса для связи между запросом и ответом, будем его инкрементить на каждую отправку
        self.request_id: int = 1

        # запомним тут параметры смены группы
        self.account_ids: Iterable[int] = []
        self.executor_group_id: Optional[int] = None

        # когда отправим последние сообщения запомним тут их request_id.
        # тогда дождавшись ответа на них мы будем уверены что всё готово
        self.final_requests = None

    async def start(self):
        """
        Основная функция клиента, в течение которой он коннектится, выполняет операции и выходит

        Параллельно запускаем cosumer и producer (https://websockets.readthedocs.io/en/stable/intro.html#both)
        consumer ожидает сообщений на вход, парсит их, добавляет в очередь на отправку
        producer спит пока очередь на отправку пуста, отправляет сообщения, если они есть, выходит когда всё готово
        """

        self.final_requests = None

        async with websockets.connect(NTPRO_WS_URL, max_size=None, ping_interval=None) as websocket:
            self.logger.info(f"Connected to: {NTPRO_WS_URL}")
            self.websocket = websocket

            self.producer_queue.append(self.get_hello_message())

            done, pending = await asyncio.wait([self.consumer(), self.producer()],
                                               return_when=asyncio.FIRST_COMPLETED)
            for future in pending:
                future.cancel()

    def get_message(self, request_type: NTProRequestType, data: Dict = None):
        if data is None:
            data = {}

        request_id = self.request_id
        self.request_id += 1

        return NTProDataMessage(RequestId=request_id, OriginalRequestId=request_id, RequestType=request_type, Msg=data)

    def get_hello_message(self):
        message = self.get_message(NTProRequestType.Hello, {
            "VersionMajor": PUBLIC_WS_API_MAJOR_VERSION,
            "VersionMinor": PUBLIC_WS_API_MINOR_VERSION,
        })
        message.OriginalRequestId = 0
        return message

    def get_login_request_message(self):
        return self.get_message(NTProRequestType.LoginRequest, {
            "UserName": NTPRO_USER,
            "Password": NTPRO_PASSWORD,
            'ClientAppType': 1
        })

    def get_select_user_role_request_message(self, role):
        return self.get_message(NTProRequestType.SelectUserRoleRequest, {'SelectedRole': role})

    def get_change_account_request_message(self, account: Dict):
        return self.get_message(NTProRequestType.ChangeAccountRequest, {'Account': account})

    def data_message_process(self, message):
        """здесь по сути находится управление клиентом, реагируя на входящие сообщения выстраиваем sequence обмена"""

        if self.final_requests and message.OriginalRequestId in self.final_requests:
            self.final_requests.remove(message.OriginalRequestId)

        if message.RequestType == NTProRequestType.AccountStatesUpdate:
            self.save_accounts_cache(message.Msg['AccountStates'])

        new_message = None

        if message.RequestType == NTProRequestType.Environment:
            # always send login after Environment reply
            new_message = self.get_login_request_message()

        if message.RequestType == NTProRequestType.LoginReply:
            # always send SelectUserRoleRequest after Login Reply
            if not message.Msg['AvailableRoles']:
                raise Exception("No AvailableRoles means bad authentication data")
            new_message = self.get_select_user_role_request_message(message.Msg['AvailableRoles'][0])

        if message.RequestType == NTProRequestType.CommandReply:
            # it may be reply to SelectUserRoleRequest then we going to do our main task
            # or it may be any other reply to commands

            reply_to = self.sent_messages[message.OriginalRequestId]

            if reply_to.RequestType != NTProRequestType.SelectUserRoleRequest:
                return

            if not self.accounts_cache:
                # we have no accounts cache so it is get_accounts sequence
                new_message = self.get_message(NTProRequestType.RequestAccountStates)
                self.final_requests = {new_message.RequestId, }
            elif self.executor_group_id is not None:
                # we have intention to change accounts
                self.final_requests = set()

                for account_id in self.account_ids:
                    account = self.accounts_cache.get(account_id, None)
                    if account is None:
                        print(f"Not found {account_id} among controlled accounts: {list(self.accounts_cache.keys())}")
                        continue

                    account['ExecutorGroupId'] = self.executor_group_id

                    new_change_message = self.get_change_account_request_message(account)
                    self.producer_queue.append(new_change_message)

                    self.final_requests.add(new_change_message.RequestId)
            else:
                raise Exception("Unknown client state")

        if new_message:
            self.producer_queue.append(new_message)

    async def consumer(self):
        while True:
            if not self.websocket:
                await asyncio.sleep(0.1)
                continue

            async for data in cast(AsyncIterable, self.websocket):
                message = await self.consumer_parser(data)

                if message.Type == NTProMessageType.PING_REQUEST:
                    self.producer_queue.append(NTProMessage(Type=NTProMessageType.PING_REPLY,
                                                            Msg=message.Msg))

                if message.Type == NTProMessageType.DATA_MESSAGE:
                    self.data_message_process(message)

    async def consumer_parser(self, data: str) -> Optional[Union[NTProDataMessage, NTProMessage]]:
        message_json = json.loads(data)
        message = NTProMessage.from_json(message_json)

        self.logger.info(f"< {message_json}")
        return message

    async def send(self, message: NTProDataMessage):
        message_json = message.to_json()
        message_data = json.dumps(message_json)
        self.logger.info(f"> {message_data}")

        await self.websocket.send(message_data)

    async def producer(self):
        while True:
            if self.final_requests is not None:
                if not self.final_requests:
                    # мы уже сформировали пул "последних запросов" и исполнили его, выходим
                    return
                print(f"Waiting {self.final_requests} to be executed")

            if self.producer_queue:
                message = self.producer_queue.pop(0)

                try:
                    await self.send(message)
                except websockets.ConnectionClosed:
                    return

                if isinstance(message, NTProDataMessage):
                    # сохраняем исходящие сообщения чтобы понимать на что нам отвечают
                    self.sent_messages[message.RequestId] = message
            else:
                await asyncio.sleep(0.1)

    async def update_executor_group(self, account_ids: Iterable[int], executor_group_id: int):
        if not self.accounts_cache:
            await self.start()

        self.account_ids = account_ids
        self.executor_group_id = executor_group_id

        await self.start()

    async def get_account_settings(self):
        if not self.accounts_cache:
            await self.start()

    def save_accounts_cache(self, account_states):
        """
        converting AccountInfo model from AccountStatesUpdate to Account

        мы отправляем только те поля которые можем поменять,
        поэтому сформируем структуру для сообщения на update
        """
        for account_state in account_states:
            account = account_state['key']
            for key in ['DirectSourceIds', 'External', 'FirmId', 'LocationId', 'OwnerFirmId']:
                account.pop(key, None)

            self.accounts_cache[account['Id']] = account