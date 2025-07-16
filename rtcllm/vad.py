import base64
from inspect import isfunction, iscoroutinefunction
from traceback import print_exc
import asyncio
import collections
import contextlib
from appPublic.folderUtils import temp_file
from appPublic.worker import awaitify
from aiortc import MediaStreamTrack
from aiortc.contrib.media import MediaBlackhole, MediaPlayer, MediaRecorder, MediaRelay
import webrtcvad
import wave
from scipy.io.wavfile import write
import numpy as np
import av
from av import AudioLayout, AudioResampler, AudioFrame, AudioFormat

def frames_write_wave(frames):
	path = temp_file(suffix='.wav')
	output_container = av.open(path, 'w')
	out_stream = output_container.add_stream('pcm_s16le')
	for frame in frames:
		for packet in out_stream.encode(frame):
			output_container.mux(packet)
	for packet in out_stream.encode(None):
		output_container.mux(packet)
	output_container.close()
	return path

def bytes2frame(byts, channels=1, sample_rate=16000):
	audio_data = np.frombuffer(byts, np.int16)
	audio_data = audio_data.reshape((channels, -1))
	layout = 'mono'
	if channels == 2:
		layout = 'stereo'
	# Create an AV frame from the audio data
	frame = av.AudioFrame.from_ndarray(audio_data, format='s16', layout='mono')
	frame.sample_rate = sample_rate
	return frame

def frame2bytes(frame):
	audio_array = frame.to_ndarray()
	dtype = audio_array.dtype
	audio_bytes = audio_array.tobytes()
	return audio_bytes

def resample(frame, sample_rate=None):
	if sample_rate is None:
		sample_rate = frame.rate
	r = AudioResampler(format='s16', layout='mono', rate=sample_rate)
	frame = r.resample(frame)
	return frame

class MyVad(webrtcvad.Vad):
	def __init__(self, callback=None):
		super().__init__(3)
		self.voiced_frames = []
		self.num_padding_frames = 40
		self.ring_buffer = collections.deque(maxlen=self.num_padding_frames)
		self.onvoiceend = callback
		self.triggered = False
		self.cnt = 0

	def voice_duration(self):
		duration = 0
		for f in self.voiced_frames:
			duration = f.samples * 1000 / f.sample_rate + duration
		return duration

	async def vad_check(self, inframe):
		"""
		ONLY SUPPORT frame with sample_rate = 16000 samples = 160
		"""
		frame = inframe
		byts = frame2bytes(frame)
		if self.cnt == 0:
			f = frame
			print(f'{f.sample_rate=}, {f.samples=},{f.layout=}, {len(byts)=}')
		if not webrtcvad.valid_rate_and_frame_length(frame.sample_rate, frame.samples):
			print('ftcygvhbunjiokmpl,mknjbhvgc')
		is_speech = self.is_speech(byts, frame.sample_rate, length=frame.samples)
		if not self.triggered:
			self.ring_buffer.append((inframe, is_speech))
			num_voiced = len([f for f, speech in self.ring_buffer if speech])
			# If we're NOTTRIGGERED and more than 90% of the frames in
			# the ring buffer are voiced frames, then enter the
			# TRIGGERED state.
			if num_voiced > 0.9 * self.ring_buffer.maxlen:
				self.triggered = True
				# We want to yield all the audio we see from now until
				# we are NOTTRIGGERED, but we have to start with the
				# audio that's already in the ring buffer.
				for f, s in self.ring_buffer:
					self.voiced_frames.append(f)
				self.ring_buffer.clear()
				# print('start voice .....', len(self.voiced_frames))
		else:
			# We're in the TRIGGERED state, so collect the audio data
			# and add it to the ring buffer.
			self.voiced_frames.append(inframe)
			self.ring_buffer.append((frame, is_speech))
			num_unvoiced = len([f for f, speech in self.ring_buffer if not speech])
			# If more than 90% of the frames in the ring buffer are
			# unvoiced, then enter NOTTRIGGERED and yield whatever
			# audio we've collected.
			if num_unvoiced > 0.9 * self.ring_buffer.maxlen:
				self.triggered = False
				duration = self.voice_duration()
				if duration > 500:
					ret = frames_write_wave(self.voiced_frames)
					if self.onvoiceend:
						if iscoroutinefunction(self.onvoiceend):
							await self.onvoiceend(ret)
						else:
							self.onvoiceend(ret)
				else:
					print('-----short voice------')


				self.ring_buffer.clear()
				self.voiced_frames = []
		self.cnt += 1

class AudioTrackVad(MediaStreamTrack):
	def __init__(self, track, stage=3, onvoiceend=None):
		super().__init__()
		self.track = track
		self.vad = MyVad(callback=onvoiceend)
		# self.sample_rate = self.track.getSettings().sampleRate
		# frameSize = self.track.getSettings().frameSize
		# self.frame_duration_ms = (1000 * frameSize) / self.sample_rate
		self.frame_duration_ms = 0.02
		self.remind_byts = b''
		self.loop = asyncio.get_event_loop()
		self.task = None
		self.debug = True
		self.running = False

	def start_vad(self):
		self.running = True
		self.task = self.loop.call_later(self.frame_duration_ms, self._recv)
	
	def _recv(self):
		asyncio.create_task(self.recv())

	def stop(self):
		self.running = False

	def to16000_160_frames(self, frame):
		frames = resample(frame, sample_rate=16000)
		ret_frames = []
		for f in frames:
			if f.samples == 160:
				return frames
		for f in frames:
			b1 = self.remind_byts + frame2bytes(f)
			while len(b1) >= 320:
				b = b1[:320]
				b1 = b1[320:]
				ret_frames.append(bytes2frame(b))
			self.remind_byts = b1
		return ret_frames

	async def recv(self):
		frame = await self.track.recv()
		self.sample_rate = frame.sample_rate
		duration = (frame.samples * 1000) / frame.sample_rate
		# print(f'{self.__class__.__name__}.recv(): {duration=}, {frame.samples=}, {frame.sample_rate=}')
		try:
			frames = self.to16000_160_frames(frame)
			for frame in frames:
				await self.vad.vad_check(frame)
		except Exception as e:
			print(f'{e=}')
			print_exc()
			return
		if self.task:
			self.task.cancel()
		if self.running:
			self.task = self.loop.call_later(self.frame_duration_ms, self._recv)
		return frame
