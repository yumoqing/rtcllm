import asyncio
from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack
from av import AudioFrame
from aiortc.contrib.media import MediaPlayer
import numpy as np

class MixedAudioTrack(MediaStreamTrack):
	kind = "audio"

	def __init__(self, tracks):
		super().__init__()
		self.tracks = tracks

	def add_track(self, track):
		if track in self.tracks:
			return
		self.tracks.append(track)
	
	de del_track(self, track):
		tracks = [ t for t in self.tracks if t != track ]
		self.tracks = tracks

	async def recv(self):
		# 获取所有音频轨道的数据
		audio_data = []
		for track in self.tracks:
			frame = await track.recv()
			if frame:
				audio_data.append(frame.to_ndarray())

		# 检查是否有有效的音频数据
		if not audio_data:
			return None

		# 将音频数据转换为 numpy 数组
		audio_arrays = [np.frombuffer(data, dtype=np.int16) for data in audio_data]

		# 确保所有音频数组长度相同
		min_length = min(len(arr) for arr in audio_arrays)
		audio_arrays = [arr[:min_length] for arr in audio_arrays]

		# 混合音频数据
		mixed_audio = np.sum(audio_arrays, axis=0, dtype=np.int16)

		# 创建新的音频帧
		new_frame = AudioFrame(format="s16", layout="stereo", samples=len(mixed_audio) // 2)
		new_frame.planes[0].update(mixed_audio.tobytes())
		new_frame.pts = self._timestamp
		self._timestamp += new_frame.samples
		return new_frame

if __name__ == '__main__':
	# 示例：创建两个音频轨道
	track1 = MediaPlayer('audio1.wav').audio
	track2 = MediaPlayer('audio2.wav').audio

	# 创建混合音频轨道
	mixed_track = MixedAudioTrack([track1, track2])

	# 创建 RTCPeerConnection 并添加混合音频轨道
	pc = RTCPeerConnection()
	pc.addTrack(mixed_track)

	# 以下部分是用于建立 WebRTC 连接的代码
	# 你可以根据需要进行修改和扩展
	async def create_offer():
		offer = await pc.createOffer()
		await pc.setLocalDescription(offer)
		print("Local description set successfully")
		# 通常在这里会发送 SDP 到对端

	# 启动异步事件循环
	loop = asyncio.get_event_loop()
	loop.run_until_complete(create_offer())
	loop.run_forever()
