import os
from traceback import print_exc
import io
import asyncio
from av import AudioFrame

from aiortc import VideoStreamTrack, AudioStreamTrack

# 计算每个样本的字节数，对于's16'是2字节，'s32'是4字节等
def bytes_to_audio_frame(bytes_data, format='s16', layout='mono', sample_rate=16000):
	"""
	从字节数据构造av.AudioFrame对象。

	参数:
	- bytes_data: 字节数据，代表音频内容。
	- format: 音频样本格式字符串，如's16'表示16位有符号整数。
	- layout: 通道布局，如'mono'、'stereo'等。
	- sample_rate: 采样率。
	"""
	# 根据给定的参数创建AudioFrame
	frame = AudioFrame(format=format, 
				layout=layout, 
				samples=len(bytes_data) // 2)

	# 将字节数据复制到AudioFrame中
	frame.planes[0].update(bytes_data)
	return frame

class LLMAudioStreamTrack(AudioStreamTrack):
	def __init__(self, omni_infer):
		super().__init__()
		self.oi = omni_infer
		self.audio_iters = []
		self.cur_iters = None
		self.tmp_files = []
	
	async def recv(self):
		try:
			b = self.get_audio_bytes()
			if b is None:
				return await super().recv()
			frame = bytes_to_audio_frame(b)
			print('LLMAudioStreamTrack return frame ...')
			return frame
		except Exception as e:
			print_exc()
			print(f'{self.__class__.__name__} recv() exception happened')
			return None

	def set_cur_audio_iter(self):
		if len(self.audio_iters) == 0:
			return False
		self.cur_iters = self.audio_iters[0]
		self.audio_iters.remove(self.cur_iters)
		return True

	def get_audio_bytes(self):
		if self.cur_iters is None:
			if not self.set_cur_audio_iter():
				return None
		try:
			b = next(self.cur_iters)
			return b
		except StopIteration:
			self.cur_iters = None
			if len(self.tmp_files) > 0:
				tf = self.tmp_files[0]
				self.tmp_files.remove(tf)
				os.remove(tf)
			return self.get_audio_bytes()

	def _feed(self, audio_file):
		self.tmp_files.append(audio_file)
		if audio_file is None:
			print(f'*****{self.__class__.__name__}._feed(),{audio_file=}')
			return
		abiters = self.oi.run_AT_batch_stream(audio_file)
		self.audio_iters.append(abiters)
		return abiters
	
