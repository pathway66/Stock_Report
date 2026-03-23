from kiwoom_collector_v3 import *
import time

api = KiwoomAPI()
api.get_token()

data, _ = api.call('ka10086', '/api/v1/domstk/stkchart', {
    'stk_cd': '005930', 'upd_stt_dt': '20260314', 'upd_end_dt': '20260320'
})
print("삼성전자 일별시세:")
print(data)