import random
from traceback import print_exc
from aiortc.contrib.media import MediaBlackhole, MediaPlayer, MediaRecorder, MediaRelay
from aiortc import MediaStreamTrack, VideoStreamTrack, AudioStreamTrack
from aiortc.mediastreams import MediaStreamError

class MyMediaPlayer(MediaPlayer):
	pass

class MyTrackBase(MediaStreamTrack):
	def __init__(self, source):
		super().__init__()
		self.dumb = None
		self.source = source
		self.set_source_track()
		print(f'{self.kind=}, {self.__class__.__name__}, {dir(self)}')

	def set_source_track(self):
		if self.kind == 'audio':
			self.track = self.source.audio
		else:
			self.track = self.source.video

	def source_reload(self):
		print(f'reload the source----{self.source._file_path}')
		self.set_source(MyMediaPlayer(self.source._file_path))

	def set_source(self, source):
		self.source = source
		self.set_source_track()

	async def recv(self):
		print(f'============{self.__class__.__name__}, {self.source.duration=}, {self.source.time=}')
		if self.source is None:
			return await self.dumb.recv()
		try:
			f = await self.track.recv()
			return f
		except MediaStreamError:
			return await self.dumb.recv()
		except Exception as e:
			print_exc()
			print(f'{e}')
			return await self.dumb.recv()
		return f

class MyAudioStreamTrack(MyTrackBase):
	kind = 'audio'
	def __init__(self, source):
		super().__init__(source)
		self.dumb = AudioStreamTrack()
		
class MyVideoStreamTrack(MyTrackBase):
	kind = 'video'
	def __init__(self, source):
		super().__init__(source)
		self.dumb = AudioStreamTrack()

"""
class MyAudioStreamTrack(AudioStreamTrack):
	def  __init__(self, source=None):
		super().__init__()
		self.source = source
		self.set_source_track()
		print(f'{self.kind=}, {self.__class__.__name__}, {dir(self)}')

	async def recv(self):
		try:
			return await self._recv()
		except MediaStreamError:
			print(f'{self.__class__.__name__} reach ended of the media ...')
			self.source_reload()
			return await self._recv()
		except Exception as e:
			print_exc()
			raise e

	async def _recv(self):
		if self.source is None:
			print('self._recv() return None')
			return None
		f = await self.track.recv()
		return f

	def set_source_track(self):
		if self.kind == 'audio':
			self.track = self.source.audio
		else:
			self.track = self.source.video

	def source_reload(self):
		print(f'reload the source----{self.source._file_path}')
		self.set_source(MyMediaPlayer(self.source._file_path))

	def set_source(self, source):
		self.source = source
		self.set_source_track()

class MyVideoStreamTrack(VideoStreamTrack):
	def  __init__(self, source=None):
		super().__init__()
		self.source = source
		self.set_source_track()
		print(f'{self.kind=}, {self.__class__.__name__}, {dir(self)}')

	async def recv(self):
		try:
			return await self._recv()
		except MediaStreamError:
			print(f'{self.__class__.__name__} reach ended of the media ...')
			self.source_reload()
			return self._recv()
		except Exception as e:
			print_exc()
			raise e

	async def _recv(self):
		if self.source is None:
			return None
		f = await self.track.recv()
		return f

	def set_source_track(self):
		if self.kind == 'audio':
			self.track = self.source.audio
		else:
			self.track = self.source.video

	def source_reload(self):
		print(f'reload the source----{self.source._file_path}')
		self.set_source(MyMediaPlayer(self.source._file_path))

	def set_source(self, source):
		self.source = source
		self.set_source_track()


class LoopingVideoTrack(VideoStreamTrack):
	'''
	A video track that loops a video file.
	'''
	def __init__(self, filename):
		super().__init__()
		self.player = MyMediaPlayer(filename)
		print(dir(self.player))

	async def recv(self):
		frame = await self.player.video.recv()
		if self.player.video.readyState != 'live':
			self.player = MyMediaPlayer(self.player._file_path)
			frame = await self.player.video.recv()
		return frame
"""

