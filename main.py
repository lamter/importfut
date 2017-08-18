import json
from mainengine import MainEngine
import traceback

settingFile = 'conf/kwargs.json'
loggingConfigFile = 'conf/logconfig.json'
# serverChanFile = 'conf/serverChan.json'

if __debug__:
    settingFile = 'tmp/kwargs.json'
    loggingConfigFile = 'tmp/logconfig.json'

# with open(serverChanFile, 'r') as f:
#     serverChanUrls = json.load(f)['serverChanSlaveUrls']

with open(settingFile, 'r') as f:
    kwargs = json.load(f)

with open(loggingConfigFile, 'r') as f:
    logConfig = json.load(f)


mainEngine = MainEngine(logconfig=logConfig, **kwargs)
mainEngine.init()
mainEngine.start()
