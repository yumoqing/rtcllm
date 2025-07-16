import aiohttp
from appPublic.dictObject import DictObject
from appPublic.oauth_client import OAuthClient

desc = {
	"path":"/api/generate.dspy",
	"method":"POST",
	"headers":[
		{
			"name":"Content-Type",
			"value":"application/json"
		}
	],
	"data":[{
		"name":"audio_file",
		"value":"${audio_file}"
	},{
		"name":"model",
		"value":"whisper"
	}
	],
	"resp":[
		{
			"name":"content",
			"value":"content"
		}
	]
}
opts = {
	"data":{
		"oops":0
	},
	"asr":desc
}

async def asr(a_file):
	if not a_file:
		return
	"""
	r = None
	with open(a_file, 'rb') as f:
		oc  = OAuthClient(DictObject(**opts))
		r = await oc('https://sage.open-computing.cn/asr', 'asr', {'audio_file':f})
		print(f'{r=}')
	return r
	"""
	with open(a_file, 'rb') as f:
		async with aiohttp.ClientSession() as session:
			async with session.post("https://sage.open-computing.cn/asr/api/generate.dspy", data={
				"model":"whisper",
				"audio_file":f
			}) as response:
				r = await response.json()
				print(f'{r=}')
				return r
	"""
	segments, info = model.transcribe(a_file, beam_size=5)
	txt = ''
	for s in segments:
		txt += s.text
	return {
		'content': txt,
		'language': info.language
	}
	"""
