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

videos = ['./1.mp4', './2.mp4']

async def pc_get_local_candidates(pc, peer):
	its = pc.__dict__.get('_RTCPeerConnection__iceTransports')
	coros =  map(lambda t: t.iceGatherer.gather(), its )
	await asyncio.gather(*coros)
	ss = pc.__dict__.get('_RTCPeerConnection__iceTransport')

	if not peer.l_candidates:
		peer.l_candidates = []
		peer.sdp_id = 0

	for t in its:
		for c in  t._connection.local_candidates:
			if c not in peer.l_candidates:
				# print(f'{c=}, {dir(c)}')
				c.sdpMid = str(peer.sdp_id)
				peer.sdp_id += 1
				peer.l_candidates.append(c)
				pc.emit('icecandidate', c)

RTCPeerConnection.get_local_candidates = pc_get_local_candidates

class RTCLLM:
	def __init__(self, ws_url, iceServers):
		self.ws_url = ws_url
		self.omni_infer = OmniInference(ckpt_dir='/d/models/mini-omni')
		self.iceServers = iceServers
		self.peers = DictObject()
		self.vid = 0
		self.info = {
			'id':'rtcllm_agent',
			'name':'rtcllm_agent'
		}
		self.dc = None
		self.loop = asyncio.get_event_loop()

	async def ws_send(self, s):
		await self.ws.send(s)

	def get_pc(self, data):
		return self.peers[data['from'].id].pc

	async def run(self):
		async with  connect(self.ws_url) as self.ws:
			await self.login()
			while True:
				msg = await self.ws.recv()
				data = DictObject(**json.loads(msg))
				d = data.data
				print(f'ws recv(): {d.type=}')
				func = self.handlers.get(d.type)
				if func:
					f = partial(func, self)
					await f(d)
			self.ws.close()

	async def login(self):
		await self.ws_send(json.dumps({
			'type':'login',
			'info':self.info
		}))

	async def on_icecandidate(self, pc, to, candidate):
		if candidate:
			candi = {
				'candidate':'candidate:' + candidate.to_sdp(),
				'sdpMid':candidate.sdpMid,
				'type': candidate.type
            }
			# print('***********on_icecandidate()', candi)
			await self.ws_send(json.dumps({
				"type":"iceCandidate",
				"to":to,
				"candidate":candi
			}))

	async def save_onlineList(self, data):
		# print(f'{self}, {type(self)}')
		self.onlineList = data.onlineList
	
	async def vad_voiceend(self, peer, audio):
		if audio is not None:
			feed = awaitify(peer.llmtrack._feed)
			ret = await feed(audio)
			print(f'self.feed("{audio}") return {ret}')
			# os.remove(audio)

	async def auto_accept_call(self, data):
		opts = DictObject(iceServers=self.iceServers)
		pc = RTCPeerConnection(opts)
		player = MyMediaPlayer('./1.mp4')
		llmtrack = LLMAudioStreamTrack(self.omni_infer)
		self.peers[data['from'].id] = DictObject(**{
			'info':data['from'],
			'llmtrack':llmtrack,
			'player':player,
			'pc':pc
		})
		pc.addTrack(llmtrack)
		# pc.addTrack(MyAudioStreamTrack(player))
		pc.addTrack(MyVideoStreamTrack(player))
		# pc.addTrack(LoopingVideoTrack('./1.mp4'))
		# pc.addTrack(player.video)
		await self.ws_send(json.dumps({'type':'callAccepted', 'to':data['from']}))
		print('auto_accept_call() end')
	
	async def pc_track(self, peerid, track):
		peer = self.peers[peerid]
		pc = peer.pc
		if track.kind == 'audio':
			f = partial(self.vad_voiceend, peer)
			vadtrack = AudioTrackVad(track, stage=3, onvoiceend=f)
			peer.vadtrack = vadtrack
			vadtrack.start_vad()

	async def pc_connectionState_changed(self, peerid):
		peer = self.peers[peerid]
		pc = peer.pc
		print(f'conn_state={pc.connectionState} ...........')
		if pc.connectionState == 'connected':
			peer.dc = pc.createDataChannel(peer.info.name)
			return
		if pc.connectionState == 'closed':
			await pc.close()
			if peer.dc:
				await peer.dc.close()
			await self.ws_send(json.dumps({
				'type':'disconnect',
				'to': peer.info
			}))
			peers = {
				k:v for k,v in self.peers.items() if k != peerid
			}
			self.peers = peers

	async def response_offer(self, data):
		pc = self.get_pc(data)
		peer = self.peers[data['from'].id]
		if pc is None:
			print(f'{self.peers=}, {data=}')
			return
		pc.on("connectionstatechange", partial(self.pc_connectionState_changed, data['from'].id))
		pc.on('track', partial(self.pc_track, data['from'].id))
		pc.on('icecandidate', partial(self.on_icecandidate, pc, data['from']))

		offer = RTCSessionDescription(** data.offer)
		await pc.setRemoteDescription(offer)
		answer = await pc.createAnswer()
		await pc.setLocalDescription(answer)
		await self.ws_send(json.dumps({
			'type':'answer',
			'answer':{'type':pc.localDescription.type, 'sdp':pc.localDescription.sdp},
			'to':data['from']
		}))
		cands = await pc.get_local_candidates(peer)

		"""
		offer = await pc.createOffer()
		await pc.setLocalDescription(offer)
		await self.ws_send(json.dumps({
			'type':'offer', 
			'offer': {'type':pc.localDescription.type, 'sdp':pc.localDescription.sdp},
			'to':data['from']
		}))
		"""
	
	async def accept_answer(self, data):
		pc = self.get_pc(data)
		answer = RTCSessionDescription(data.answer);
		pc.setRemoteDescription(answer)
	
	async def accept_iceCandidate(self, data):
		pc = self.get_pc(data)
		candidate = data.candidate
		# print('accepted candidate=', candidate)
		"""
		rtc_candidate = RTCIceCandidate(
			ip=ip,
			port=port,
			protocol=protocol,
			priority=priority,
			foundation=foundation,
			component=component,
			type=type,
			sdpMid=candidate['sdpMid'],
			sdpMLineIndex=candidate['sdpMLineIndex']
		)
		"""
		rtc_candidate = candidate_from_sdp(candidate['candidate'].split(":", 1)[1])
		rtc_candidate.sdpMid = candidate['sdpMid']
		rtc_candidate.sdpMLineIndex = candidate['sdpMLineIndex']
		await pc.addIceCandidate(rtc_candidate)
		# print('addIceCandidate ok')

	handlers = {
		'onlineList':save_onlineList,
		'callRequest':auto_accept_call,
		'offer':response_offer,
		'answer':accept_answer,
		'iceCandidate':accept_iceCandidate,
	}

async def main():
	hf_socks5proxy()
	agent = RTCLLM(ws_url='wss://sage.open-computing.cn/wss/ws/rtc_signaling.ws',
					iceServers=[{
						'urls':'stun:stun.open-computing.cn:13478'},{
						'urls':'turn:stun.open-computing.cn:13479',
						'username':'turn',
						'credential':'server'
					}])
	print('running ...')
	await agent.run()

if __name__ == '__main__':
	asyncio.run(main())

