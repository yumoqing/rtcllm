import pyaudio
import av
import numpy as np
from vad import frames_write_wave
import sys
import select
from vad import MyVad, bytes2frame, to16000_160_frames

def check_input():
    if select.select([sys.stdin], [], [], 0.0)[0]:
        input_str = sys.stdin.readline().strip()
        return input_str
    else:
        return None

def mic(seconds=None, frames_ms=20):
	# PyAudio setup
	# 
	p = pyaudio.PyAudio()

	# Audio format
	FORMAT = pyaudio.paInt16  # 16-bit resolution
	CHANNELS = 1			  # 1 channel for mono
	RATE = 16000			  # 44.1kHz sampling rate
	CHUNK = int(RATE / 100) * 2		  # 1024 samples per frame
	# PyAudio input stream
	stream = p.open(format=FORMAT,
					channels=CHANNELS,
					rate=RATE,
					input=True,
					frames_per_buffer=CHUNK)

	print("Recording and streaming audio...")
	if seconds is not None:
		cnt = int(seconds * 1000 / frames_ms)
	else:
		cnt = 0
	i = 0
	while True:
		if cnt == 0:
			if check_input() == 'q':
				break
		else:
			if i>=cnt:
				break
		# Read audio frames from the microphone
		data = stream.read(CHUNK)
		# print(f'{data.__class__.__name__}, {len(data)=}, {CHUNK=}')
		frames = to16000_160_frames(bytes2frame(data))
		for frame in frames:
			yield frame
		i += 1

	# Close the audio stream and the container
	stream.close()
	p.terminate()

if __name__ == '__main__':
	def cb(f):
		print(f'{f} voice wave file')

	i = 0
	vad = MyVad(callback=cb)
	for f in mic():
		if i == 0:
			print(f'{f.sample_rate=}, {f.samples=}, {f.layout=} ')
		i += 1
		vad.vad_check(f)
	
	print(f'record save to {f}')
