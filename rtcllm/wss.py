
from websockets.client import connect
import websockets
from appPublic.background import Background
from appPublic.uniqueID import getID
from appPublic.dictObject import DictObject

class Wss:
	"""
	new session
		session_type: p2p, aba, meeting
	
	signaling data format
	create a new session
	{
		type:new_session,
		session:{
			sessiontype:
			sessionid
		}
		from:
		to:
	}
	(will boastcast to all peer online)login:
	{
		type:login,
		from:
	}
	(will boastcast to all peer online)logout:
	{
		type:logout
		from:
	}
	"""
	def __init__(self, app, wss_url, info=None, opts={}):
		self.wss_url = wss_url
		if info is None:
			info = {
				'id':getID()
			}
		self.opts = opts
		self.info = info
		self.ws = None
		self.handler_name = 'sessionid'
		self.app = app
		self.running = False
		self.app.wss = self
		self.handlers = {}
		self.sessionhandlers = {}
		self.peers = []

	def add_handler(self, key, handler):
		sef.handlers[key] = handler

	def start(self):
		loop = asyncio.get_running_loop()
		f = asyncio.run_coroutine_threadsafe(self._start, loop)
		return f.result()

	def add_sessionhandler(self, sessiontype, handler):
		self.sessionhandlers[sessiontype] = handler
	
	async def _start(self):
		self.ws = await connect(self.wss_url)
		self.running = True
		while self.running:
			msg = await self.ws.recv()
			data = DictObject(**json.loads(msg))
			name = data.get(self.handler_name)
			handler = self.handlers.get(name)
			if handler is None:
				handler = self.recvdata_handler
			await handler(data)
	
	def add_peer(self, info):
		self.peers = [ i for i in self.peers if i.id != info.id ]
		self.peers.append(info)
	
	def delete_peer(self, info):
		self.peers = [ i for i in self.peers if i.id != info.id ]
	
	async def recvdata_handler(self, data):
		if data.type == 'login':
			self.add_peer(data.msgfrom)
			return
		if data.type == 'logout':
			self.delete_peer(data.msgfrom)
			return
		if data.type == 'new_session'
			h = self.sessionhandlers.get(data.session.sessiontype)
			i = h(data.from)
			k = data.session.sessionid
			self.add_handler(k, i.recvdata_handler)
			return

	async def stop(self):
		self.running = False
		await self.ws.close()

	async def restart(self):
		try:
			await self.stop()
		except:
			pass
		await self.start()

	async def send(self, dic):
		dic.msgfrom = self.info
		txt = json.dumps(dic)
		try:
			return await self.ws.send(txt)
		except:
			await self.restart()
			return self.send(dic)
	
	async def new_session(self, sessiontype, opts):
		k = self.sessionhandlers.get(sessiontype)
		if k is None:
			e = Exception(f'Sessiontype({sessiontype}) not registed')
			exception(f'new_session() exception {e}')
			raise e
		d = DictObject()
		d.type = 'new_session'
		d.msgfrom = self.info
		sessionid = getID()
		d.session = DictObject(sessiontype=sessiontype, sessionid=sessionid)
		await self.send(d)
		h = k(self, sessionid, opts)
		self.add_sessionhandler(sessionid, h.recvdata_handler)
		return h

	async def login(self):
		d = DictObject()
		d.msgfrom = self.info
		d.type = 'login'
		await self.send(d)
	
	async def logout(self):
		d = DictObject()
		d.msgfrom = self.info
		d.type = 'logout'
		await self.send(d)
