import os
import sys
sys.path.append('./mini_omni')

import asyncio
import random
import json

from functools import partial

from appPublic.dictObject import DictObject
from appPublic.hf import hf_socks5proxy
from appPublic.worker import awaitify
from appPublic.log import debug, error, exception, critical
from aiortc import MediaStreamTrack, VideoStreamTrack, AudioStreamTrack, RTCPeerConnection, RTCSessionDescription, RTCIceCandidate
from aiortc.sdp import candidate_from_sdp, candidate_to_sdp
from aiortc.contrib.media import MediaBlackhole, MediaPlayer, MediaRecorder, MediaRelay

# from websockets.asyncio.client import connect
# from websockets.asyncio.client import connect
from websockets.client import connect
import websockets
from stt import asr
from vad import AudioTrackVad
from a2a import LLMAudioStreamTrack
from aav import MyMediaPlayer, MyAudioStreamTrack, MyVideoStreamTrack

from mini_omni.inference import OmniInference



class RTCSignaling:
	/*
	{
		signaling_url:
		info:
		connect_opts:
		onclose:
		onlogin:
	}
	*/
	def __init__(self, opts):
		self.signaling_url = opts.signaling_url
		self.info = opts.info
		self.connect_opts = opts.connect_opts
		self.peers = []
		self.sessions = DictObject()
		self.handlers = DictObject()
		self.sessionhandlers = DictObject()
		self.init_websocket()
		self.hb_task = None
		self.socket = None
		self.heartbeat_period = opts.heartbeat_period
		self.onclose = opts.onclose
		self.onlogin = opts.onlogin
		self.onopen = opts.onopen
		if not self.heartbeat_period:
			self.heartbeat_period = 0
	
	async def run(self):
		async with  connect(self.ws_url) as self.socket:
			await self.login()
			while True:
				msg = await self.socket.recv()
				data = DictObject(**json.loads(msg))
				d = data.data
				debug(f'ws recv(): {d.type=}')
				await self.signaling_recvdata(d)
			await self.socket.close()

	def reconnect(self):
		debug('eror happened')
		if self.hb_task:
			self.hb_task.cancel()
			self.hb_task = None
		
		if self.onclose:
			self.onclose()
		
		return
	
	def del_handler(self, sessionid):
		handlers = { k:v \
				for k,v in self.handlers.items() if k!=sessionid}
		self.handlers = handlers
	
	async def signaling_recvdata(self, data):
		debug(f'ws recv data={data}')
		if data.session:
			sessionid = data.session.sessionid
			sessiontype = data.session.sessiontype
			handler = self.handlers[sessionid]
			if not handler:
				k = self.sessionhandlers[sessiontype]
				if not k:
					e = Exception('recvdata_handler() exception(' + sessiontype + ')')
					raise e
				
				h = k(self, data.session, self.connect_opts)
				handler = h.recvdata_handler.bind(h)
				self.add_handler(sessionid, handler)
			
			await handler(data)
		else:
			await self.recvdata_handler(data)

	async def add_handler(self, key, handler):
		self.handlers[key] = handler
	
	async def add_sessionhandler(self, sessiontype, handler):
		self.sessionhandlers[sessiontype] = handler
	
	async def recvdata_handler(self, data):
		debug(f'recv data={data}')
		if data.type == 'online'):
			for p in data.online:
				d = self.peers[p.id]
				if (!d) d = DictOject()
				d.update(p)
				self.peers[p.id] = d
			
			if self.onlogin:
				self.onlogin(data.online)
			
			return
		
		debug(f'recv data= {data} NOT HANDLED')
	
	async def new_session(self, sessiontype, peer):
		k = self.sessionhandlers[sessiontype]
		if not k:
			e = Exception('new_session() exception(' + sessiontype + ')')
			raise e
		sessionid = getID()
		session = DictObject(**{
			sessiontype:sessiontype,
			sessionid:sessionid
		})
		d = DictObject(**{
			"type":'new_session',
			"session": session
		})
		await self.send_data(d)
		opts = self.connect_options.clone()
		opts.peer_info = peer
		h = k(self, session, opts)
		self.add_handler(sessionid, h.recvdata_handler)
		return h

	async def login(self):
		debug(f'login send {self.heartbeat_period=}')
		d = DictObject(
			type='login'
		)
		self.send_data(d)
		if self.heartbeat_period > 0):
			debug(f'call login again in {self.heartbeat_period} seconds')
			self.hb_task = schedule_once(self.login.bind(self), self.heartbeat_period)
		

	async def logout(self):
		d = DictObject(
			type='logout'
		)
		await self.send_data(d)
	
	await send_data(self, d):
		d.msgfrom = self.info
		s = json.dumps(d)
		debug(f'send_data()  {s=}')
		await self.socket.send(s)
	
	async def socket_send(self, s):
		await self.socket.send(s)

class RTCP2PConnect:
	/*
	opts:{
		ice_servers:
		peer_info:
		auto_callaccept: true or false
		media_options: { video:trur or false, audio:true or false }
		data_connect: true or false
	}
	*/

	def __init__(self, signaling, session, opts):
		self.id = bricks.uuid()
		self.signaling = signaling
		self.session = session
		self.requester = false
		self.opts = opts
		self.peers = DictObject()
		self.signal_handlers = DictObject()
		self.local_stream = None
		self.localVideo = None
		self.add_handler('sessioncreated', self.h_sessioncreated)
		self.add_handler('callrequest', self.h_callrequest)
		self.add_handler('callaccepted', self.h_callaccepted)
		self.add_handler('offer', self.h_offer)
		self.add_handler('answer', self.h_answer)
		self.add_handler('icecandidate', self.h_icecandidate)
		self.add_handler('sessionquit', self.h_sessionquit)

	def add_handler(self, typ, f):
		self.signal_handlers[typ] = f
	
	def get_handler(self, typ):
		return self.signal_handlers[typ]
	

	async def p2pconnect(self, peer):
		await self.getLocalStream()
		p = self.peers.get(peer.id)
		if not p:
			await self.createPeerConnection(peer)
		else:
			debug(f'{peer=}, connect exists {self=}')
		
		debug(f'p2pconnect() called {self=} {peer=}')
	
	async def h_sessioncreated(self, data):
		await self.p2pconnect(self.opts.peer_info, 'requester')
		if self.opts.peer_info:
			d = DictObject(**{
				"type":'callrequest',
				"msgto":self.opts.peer_info
			})
			await self.signaling_send(d)
			self.requester = true

	async def h_callrequest(self, data):
		if self.opts.auto_callaccept or true):
			await self.p2pconnect(data.msgfrom, 'responser')
			d = DictObject(**{
				"type":'callaccepted',
				"msgto":data.msgfrom
			})
			await self.signaling_send(d)
			return

	async def h_callaccepted(self, data):
		self.createDataChannel(data.msgfrom)
		await self.send_offer(data.msgfrom, true)
	
	async def h_offer(self, data):
		debug(f'h_offer(), {self=}, peer={data.msgfrom}')
		pc = self.peers[data.msgfrom.id].pc
		offer = RTCSessionDescription(data.offer)
		await pc.setRemoteDescription(offer)
		answer = await pc.createAnswer()
		await pc.setLocalDescription(answer)
		d = DictObject(**{
			type:'answer',
			answer:pc.localDescription,
			msgto:data.msgfrom
		})
		await self.signaling_send(d)
		if self.peers[data.msgfrom.id].role != 'requester':
			await self.send_offer(data.msgfrom)
		
	async def h_answer(self, data):
		desc = RTCSessionDescription(data.answer)
		pc = self.peers[data.msgfrom.id].pc
		await pc.setRemoteDescription(desc)
	
	async def h_icecandidate(self, data):
		candidate = RTCIceCandidate(data.candidate)
		pc = self.peers[data.msgfrom.id].pc
		await pc.addIceCandidate(candidate)
	
	async def h_sessionquit(self, data):
		pc = self.peers[data.msgfrom.id].pc
		self.peer_close(data.msgfrom)
	
	async send_offer(self, peer, initial){
		debug(f'send_offer(), peers={self.peers}, {peer=}')
		pc = self.peers[peer.id].pc
		if initial:
			self.peers[peer.id].role = 'requester'
		else:
			self.peers[peer.id].role = 'responser'
		
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)
        d = DictObject(**{
            "type":'offer',
            "offer":pc.localDescription,
            "msgto":peer
        })
		await self.signaling_send(d)
	
	async def send_candidate(self, peer, event):
		debug(f'send_candidate called, {peer=},{event=}') 
        if (event.candidate) {
            candidate = event.candidate
            self.signaling_send({ 
                type: 'icecandidate', 
                msgto:peer,
                candidate: candidate
            }) 
        }
	}

	signaling_send(d){
		d.session = self.session
		self.signaling.send_data(d)
	}

	async recvdata_handler(data){
		debug('recvdata=', data, self.signal_handlers)
		f = self.get_handler(data.type)
		if (f) {
			await f(data)
			return
		}
		debug('recvdata=', data, 'NOT HANDLED')
	}

	async ice_statechange(peer, event){
		pc = self.peers[peer.id].pc
		debug(`oniceconnectionstatechange, pc.iceConnectionState is ${pc.iceConnectionState}.`)
	}

	async connection_statechange(peer, event){
		pc = self.peers[peer.id].pc
		debug(`${peer.id} state changed. new state=${pc.connectionState}`)
		debug('state=', pc.connectionState, typeof(pc.connectionState))
		if (pc.connectionState == 'disconnected'){
			self.peer_close(peer)
			if (self.opts.on_pc_disconnected){
				self.opts.on_pc_disconnected(peer)
			}
            return
        }
        if (pc.connectionState == 'connected'){
			debug('state is connected, data_connect=',
						self.opts.data_connect)
			if(self.opts.on_pc_connected){
				self.opts.on_pc_connected(peer)
			}
        }
	}

	async dc_accepted(peer, event){
		debug('accept datachannel ....')
		self.peers[peer.id].dc = event.channel
		await self.dc_created(peer, self.peers[peer.id].dc)
	}
	async dc_created(self, peer, dc):
		debug('dc_created.....', dc)
		dc.onmessage = self.datachannel_message.bind(peer)
		dc.onopen = self.datachannel_open(peer)
		dc.onclose = self.datachannel_close(peer)

	async datachannel_message(self, peer, event):
		debug('datachannel_message():', self, arguments)
		dc = self.peers[peer.id].dc
		if self.opts.on_dc_messaage:
			await self.opts.on_dc_message(dc, event.data)
	
	async def datachannel_open(self, peer):
		debug('datachannel_open():', self, arguments)
		dc = self.peers[peer.id].dc
		if self.opts.on_dc_open:
			await self.opts.on_dc_open(dc)

	async def datachannel_close(self, peer):
		debug('datachannel_close():', self, arguments)
		dc = self.peers[peer.id].dc
		if self.opts.on_dc_close:
			await self.opts.on_dc_close(dc)
	
	async def createDataChannel(self, peer):
		pc = self.peers[peer.id].pc
		self.peers[peer.id].dc = pc.createDataChannel('chat', {ordered:true})
		dc = self.peers[peer.id].dc
		await self.dc_created(peer, self.peers[peer.id].dc)
		debug('dc created', self.peers[peer.id].dc)
	
	async def createPeerConnection(self, peer):
		configuration = DictObject(**{
			"iceServers":self.opts.ice_servers
		})
		debug(f'RTCPC {configuration=}')
		pc = RTCPeerConnection(configuration)
		if self.local_stream:
			for track in self.local_stream.getTracks():
				pc.addTrack(track, self.local_stream)
		self.peers[peer.id] = peer
		self.peers[peer.id].pc = pc
		self.peers[peer.id].role = ''
        pc.onicecandidate = partial(self.send_candidate, peer)
        pc.oniceconnectionstatechange = \
					partial(self.ice_statechange, peer)
        pc.onconnectionstatechange = \
					partial(self.connection_statechange, peer)
		pc.ondatachanel = partial(self.dc_accepted, peer)
		pc.ontrack = partial(self.onpctrack, peer)
	
	def peer_close(self, peer){
		pc = self.peers[peer.id].pc
		video = self.peers[peer.id].video
		for r in pc.getReceivers():
			r.track.stop()
		
		for s in pc.getSenders():
			s.track.stop()
		
		dc = self.peers[peer.id].dc
		if (dc){
			dc.close()
		}
		pc.close()
		self.peers = {k:v for k,v in self.peers.items() if k!=peer.id}
		cnt = len(self.peers.keys())
		if (keys.length == 0){
			self.localVideo.get_stream()
			.getTracks().forEach(track => track.stop())
			self.local_stream.getTracks().forEach(track => track.stop())
			self.local_screan.getTracks().forEach(track => track.stop())
			self.signaling.del_handler(self.session.sessionid)
		}
	}
}

