import requests
import pandas as pd
import numpy as np
import json
import time
import math
import traceback
import dateutil.parser
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from copy import copy
from threading import Thread
from datetime import datetime, timedelta
from app import tradelib as tl


class Subscription(object):
	ACCOUNT = 'account'
	CHART = 'chart'

	def __init__(self, broker, msg_id, sub_type, *args):
		self.broker = broker
		self.msg_id = msg_id

		self.res = []
		self.sub_type = sub_type
		self.args = args

		self.receive = False
		self.stream = None
		self.last_update = None

	def setStream(self, stream):
		self.receive = True
		self.stream = stream


	def onUpdate(self, *args):
		# print(f'ON UPDATE: {args}', flush=True)

		self.broker._send_response(
			self.msg_id,
			{
				'args': args,
				'kwargs': {}
			}
		)


class Oanda(object):

	def __init__(self, 
		container, user_id, broker_id, key, is_demo, accounts={}, is_parent=False, is_dummy=False
	):

		self.container = container
		self.userId = user_id
		self.brokerId = broker_id
		self.accounts = accounts
		self._key = key
		self._is_demo = is_demo
		self._is_connected = False

		print(f'SET OANDA {self._key}, {self._is_demo}', flush=True)

		self._session = requests.session()
		self._headers = {
			'Authorization': 'Bearer '+self._key,
			'Connection': 'keep-alive',
			'Content-Type': 'application/json'
		}
		self._session.headers.update(self._headers)

		self._url = (
			'https://api-fxpractice.oanda.com'
			if self._is_demo else
			'https://api-fxtrade.oanda.com'
		)
		self._stream_url = (
			'https://stream-fxpractice.oanda.com'
			if self._is_demo else
			'https://stream-fxtrade.oanda.com'
		)

		self._last_update = time.time()
		self._subscriptions = []


	def set(self, user_id, broker_id, key, is_demo, accounts, is_parent, is_dummy):
		self.userId = user_id
		self.brokerId = broker_id
		self.accounts = accounts
		self._key = key
		self._is_demo = is_demo

		self._headers = {
			'Authorization': 'Bearer '+self._key,
			'Connection': 'keep-alive',
			'Content-Type': 'application/json'
		}
		self._session.headers.update(self._headers)

		self._url = (
			'https://api-fxpractice.oanda.com'
			if self._is_demo else
			'https://api-fxtrade.oanda.com'
		)
		self._stream_url = (
			'https://stream-fxpractice.oanda.com'
			if self._is_demo else
			'https://stream-fxtrade.oanda.com'
		)


	def _periodic_check(self):
		TWENTY_SECONDS = 20
		self._last_update = time.time()
		# Check most recent Oanda `HEARTBEAT` was received or reconnect
		while self.is_running:
			if time.time() - self._last_update > TWENTY_SECONDS:
				print('RECONNECT', flush=True)
				if self._is_connected:
					self._is_connected = False
					# Run disconnected callback
					# self.handleOnSessionStatus({
					# 	'broker': self.name,
					# 	'timestamp': math.floor(time.time()),
					# 	'type': 'disconnected',
					# 	'message': 'The session has been disconnected.'
					# })

					# Perform periodic refresh
					self._reconnect()
			time.sleep(5)

		for sub in self._subscriptions:
			for i in sub.res:
				i.close()


	def _send_response(self, msg_id, res):
		res = {
			'msg_id': msg_id,
			'result': res
		}

		self.container.sio.emit(
			'broker_res', 
			res, 
			namespace='/broker'
		)


	def _download_historical_data_broker(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		force_download=False
	):
		result = {}

		dl_start = None
		dl_end = None
		if start:
			dl_start = start
			start = tl.utils.convertTimestampToTime(start)
		if end:
			dl_end = end
			end = tl.utils.convertTimestampToTime(end)

		# if period == tl.period.TICK:
		# 	period = tl.period.FIVE_SECONDS

		while True:
			# time.sleep(0.5)

			if count:
				if start:
					start = tl.utils.convertTimezone(start, 'UTC')

					start_str = start.strftime('%Y-%m-%dT%H:%M:%S.000000000Z')
					endpoint = '/v3/instruments/{}/candles?price=BAM' \
								'&from={}&count={}&granularity={}&smooth=True'.format(
									product, start_str, count, period
								)
				else:
					endpoint = '/v3/instruments/{}/candles?price=BAM' \
								'&count={}&granularity={}&smooth=True'.format(
									product, count, period
								)
			else:
				start = tl.utils.convertTimezone(start, 'UTC')
				end = tl.utils.convertTimezone(end, 'UTC')

				start_str = start.strftime('%Y-%m-%dT%H:%M:%S.000000000Z')
				end_str = end.strftime('%Y-%m-%dT%H:%M:%S.000000000Z')
				endpoint = '/v3/instruments/{}/candles?price=BAM' \
							'&from={}&to={}&granularity={}&smooth=True'.format(
								product, start_str, end_str, period
							)

			res = self._session.get(
				self._url + endpoint,
				headers=self._headers
			)

			if res.status_code == 200:
				if len(result) == 0:
					result['timestamp'] = []
					result['ask_open'] = []
					result['ask_high'] = []
					result['ask_low'] = []
					result['ask_close'] = []
					result['mid_open'] = []
					result['mid_high'] = []
					result['mid_low'] = []
					result['mid_close'] = []
					result['bid_open'] = []
					result['bid_high'] = []
					result['bid_low'] = []
					result['bid_close'] = []

				data = res.json()
				candles = data['candles']

				for i in candles:

					dt = datetime.strptime(i['time'], '%Y-%m-%dT%H:%M:%S.000000000Z')
					ts = tl.utils.convertTimeToTimestamp(dt)

					if ((not dl_start or ts >= dl_start) and 
						(not dl_end or ts < dl_end) and 
						(not len(result['timestamp']) or ts != result['timestamp'][-1])):
					
						result['timestamp'].append(ts)
						asks = list(map(float, i['ask'].values()))
						mids = list(map(float, i['mid'].values()))
						bids = list(map(float, i['bid'].values()))
						result['ask_open'].append(asks[0])
						result['ask_high'].append(asks[1])
						result['ask_low'].append(asks[2])
						result['ask_close'].append(asks[3])
						result['mid_open'].append(mids[0])
						result['mid_high'].append(mids[1])
						result['mid_low'].append(mids[2])
						result['mid_close'].append(mids[3])
						result['bid_open'].append(bids[0])
						result['bid_high'].append(bids[1])
						result['bid_low'].append(bids[2])
						result['bid_close'].append(bids[3])

				if count:
					if (not len(result['timestamp']) >= 5000 and
							start and end and not self._is_last_candle_found(period, start, end, count)):
						start = datetime.strptime(candles[-1]['time'], '%Y-%m-%dT%H:%M:%S.000000000Z')
						count = 5000
						continue
				
				return pd.DataFrame(data=result).set_index('timestamp').to_dict()

			if res.status_code == 400:
				if (
					'Maximum' in res.json()['errorMessage'] or 
					('future' in res.json()['errorMessage'] and
						'\'to\'' in res.json()['errorMessage'])
				):
					count = 5000
					continue
				else:
					if len(result):
						return pd.DataFrame(data=result).set_index('timestamp').to_dict()
					else:
						print(res.json())
						return None
			else:
				print('Error:\n{0}'.format(res.json()))
				return None


	def _is_last_candle_found(self, period, start_dt, end_dt, count):
		utcnow = tl.utils.setTimezone(datetime.utcnow(), 'UTC')
		if period == tl.period.ONE_MINUTE:
			new_dt = start_dt + timedelta(minutes=count)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.TWO_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*2)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.THREE_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*3)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.FIVE_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*5)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.TEN_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*10)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.FIFTEEN_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*15)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.THIRTY_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*30)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.ONE_HOUR:
			new_dt = start_dt + timedelta(hours=count)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.FOUR_HOURS:
			new_dt = start_dt + timedelta(hours=count*4)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.DAILY:
			new_dt = start_dt + timedelta(hours=count*24)
			return new_dt >= end_dt or new_dt >= utcnow
		else:
			raise Exception('Period not found.')

	# Order Requests


	def _get_all_positions(self, account_id):
		endpoint = f'/v3/accounts/{account_id}/openTrades'
		res = self._session.get(
			self._url + endpoint,
			headers=self._headers
		)

		if res.status_code == 200:
			result = {account_id: []}
			res = res.json()
			print(res, flush=True)
			for pos in res.get('trades'):
				order_id = pos.get('id')
				product = pos.get('instrument')
				direction = tl.LONG if float(pos.get('currentUnits')) > 0 else tl.SHORT
				lotsize = self.convertToLotsize(abs(float(pos.get('currentUnits'))))
				entry_price = float(pos.get('price'))
				sl = None
				sl_id = None
				if pos.get('stopLossOrder'):
					sl = float(pos['stopLossOrder'].get('price'))
					sl_id = pos['stopLossOrder'].get('id')
				tp = None
				tp_id = None
				if pos.get('takeProfitOrder'):
					tp = float(pos['takeProfitOrder'].get('price'))
					tp_id = pos['takeProfitOrder'].get('id')
				open_time = datetime.strptime(pos.get('openTime').split('.')[0], '%Y-%m-%dT%H:%M:%S')

				new_pos = tl.Position(
					self,
					order_id, account_id, product,
					tl.MARKET_ENTRY, direction, lotsize,
					entry_price, sl, tp, 
					tl.utils.convertTimeToTimestamp(open_time),
					sl_id=sl_id, tp_id=tp_id
				)

				result[account_id].append(new_pos)

			print(result, flush=True)
			return result
		else:
			return None

	def authCheck(self, account_id):
		result = self._get_all_positions(account_id)

		if result is not None:
			return {'result': True}
		else:
			return {'result': False}


	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):

		# Flip lotsize if direction is short
		if direction == tl.SHORT: lotsize *= -1

		# Convert to unit size
		lotsize = self.convertToUnitSize(lotsize)

		payload = {
			'order': {
				'instrument': product,
				'units': str(int(lotsize)),
				'type': 'MARKET',
				'timeInForce': 'FOK',
				'positionFill': 'DEFAULT',
			}
		}


		endpoint = f'/v3/accounts/{account_id}/orders'
		print(f'CREATING POSITION: {endpoint}, {payload}', flush=True)
		res = self._session.post(
			self._url + endpoint,
			headers=self._headers,
			data=json.dumps(payload)
		)

		result = {}
		status_code = res.status_code
		res = res.json()

		print(f'CREATING POSITION DONE: {status_code}, {res}', flush=True)
		
		return {'status': status_code, 'result': res}


	def modifyPosition(self, pos, sl_price, tp_price):

		payload = {}

		if sl_price is None:
			payload['stopLoss'] = {
				'timeInForce': 'GTC',
				'price': sl_price
			}

		elif sl_price != pos['sl']:
			sl_price = str(round(sl_price, 5))
			payload['stopLoss'] = {
				'timeInForce': 'GTC',
				'price': sl_price
			}

		if tp_price is None:
			payload['takeProfit'] = {
				'timeInForce': 'GTC',
				'price': tp_price
			}

		elif tp_price != pos['tp']:
			tp_price = str(round(tp_price, 5))
			payload['takeProfit'] = {
				'timeInForce': 'GTC',
				'price': tp_price
			}
				
		if len(payload):
			endpoint = f'/v3/accounts/{pos["account_id"]}/trades/{pos["order_id"]}/orders'
			print(f'MODIFY POSITION: {endpoint}, {payload}', flush=True)
			res = self._session.put(
				self._url + endpoint,
				headers=self._headers,
				data=json.dumps(payload)
			)
		else:
			raise Exception('No specified stop loss or take profit to modify.')

		result = {}
		status_code = res.status_code
		res = res.json()
		print(f'MODIFY POSITION DONE: {status_code}, {res}', flush=True)

		return {'status': status_code, 'result': res}


	def deletePosition(self, pos, lotsize):

		if lotsize >= pos['lotsize']: 
			units = 'ALL'
		else: 
			# Convert to unit size
			lotsize = self.convertToUnitSize(lotsize)
			units = str(int(lotsize))

		payload = {
			'units': units
		}

		endpoint = f'/v3/accounts/{pos["account_id"]}/trades/{pos["order_id"]}/close'
		print(f'DELETE POSITION: {endpoint}, {payload}', flush=True)
		res = self._session.put(
			self._url + endpoint,
			headers=self._headers,
			data=json.dumps(payload)
		)

		result = {}
		status_code = res.status_code
		res = res.json()
		print(f'DELETE POSITION DONE: {status_code}, {res}', flush=True)

		return {'status': status_code, 'result': res}

	def _get_all_orders(self, account_id):
		endpoint = f'/v3/accounts/{account_id}/pendingOrders'
		res = self._session.get(
			self._url + endpoint,
			headers=self._headers
		)

		if res.status_code == 200:
			result = {account_id: []}
			res = res.json()
			for order in res.get('orders'):
				if order.get('type') == 'LIMIT' or order.get('type') == 'STOP':
					order_id = order.get('id')
					product = order.get('instrument')
					direction = tl.LONG if float(order.get('units')) > 0 else tl.SHORT
					lotsize =  self.convertToLotsize(abs(float(order.get('units'))))
					entry_price = float(order.get('price'))
					sl = None
					if order.get('stopLossOnFill'):
						sl = float(order['stopLossOnFill'].get('price'))
					tp = None
					if order.get('takeProfitOnFill'):
						tp = float(order['takeProfitOnFill'].get('price'))
					open_time = datetime.strptime(order.get('createTime').split('.')[0], '%Y-%m-%dT%H:%M:%S')

					if order.get('type') == 'LIMIT':
						order_type = tl.LIMIT_ORDER
					elif order.get('type') == 'STOP':
						order_type = tl.STOP_ORDER

					new_order = tl.Order(
						self,
						order_id, account_id, product, order_type,
						direction, lotsize, entry_price, sl, tp,
						tl.convertTimeToTimestamp(open_time)
					)

					result[account_id].append(new_order)

			return result
		else:
			return None


	def getAllAccounts(self):
		print('GET ALL ACCOUNTS', flush=True)
		print(self._headers, flush=True)

		endpoint = f'/v3/accounts'
		res = self._session.get(
			self._url + endpoint,
			headers=self._headers
		)

		result = []
		status_code = res.status_code
		data = res.json()
		print(data, flush=True)
		if 200 <= status_code < 300:
			print('200', flush=True)
			for account in data['accounts']:
				if 'MT4' in account['tags'] or 'MT5' in account['tags']:
					continue

				result.append(account.get('id'))

			print(result, flush=True)
			return result
		else:
			return None


	def getAccountInfo(self, account_id):

		endpoint = f'/v3/accounts/{account_id}'
		res = self._session.get(
			self._url + endpoint,
			headers=self._headers
		)

		result = {}
		status_code = res.status_code
		res = res.json()
		if 200 <= status_code < 300:
			result[account_id] = {
				'currency': res['account'].get('currency'),
				'balance': float(res['account'].get('balance')),
				'pl': float(res['account'].get('pl')),
				'margin': float(res['account'].get('marginUsed')),
				'available': float(res['account'].get('balance')) + float(res['account'].get('pl'))
			}
			
		elif 400 <= status_code < 500:
			# Response error
			msg = 'No message available.'
			if res.get('errorMessage'):
				msg = res.get('errorMessage')

			raise Exception(msg)

		else:
			raise Exception('Oanda internal server error')

		return result

	def createOrder(self, 
		product, lotsize, direction,
		account_id, order_type, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		# Flip lotsize if direction is short
		lotsize = self.convertToUnitSize(lotsize)
		if direction == tl.SHORT: 
			lotsize *= -1

		# Convert `entry_range` to `entry_price`
		if entry_range:
			entry_range = tl.convertToPrice(entry_range)
			if order_type == tl.LIMIT_ORDER:
				if direction == tl.LONG:
					entry_price = round(self.getAsk(product) - entry_range, 5)
				else:
					entry_price = round(self.getBid(product) + entry_range, 5)

			elif order_type == tl.STOP_ORDER:
				if direction == tl.LONG:
					entry_price = round(self.getAsk(product) + entry_range, 5)
				else:
					entry_price = round(self.getBid(product) - entry_range, 5)



		# Convert order_type to Oanda readable string
		payload_order_type = None
		if order_type == tl.LIMIT_ORDER:
			payload_order_type = 'LIMIT'
		elif order_type == tl.STOP_ORDER:
			payload_order_type = 'STOP'

		payload = {
			'order': {
				'price': str(entry_price),
				'instrument': product,
				'units': str(int(lotsize)),
				'type': payload_order_type,
				'timeInForce': 'GTC',
				'positionFill': 'DEFAULT',
			}
		}
		
		if sl_price:
			payload['order']['stopLossOnFill'] = {
				'price': str(round(sl_price, 5))
			}

		elif sl_range:
			sl_range = tl.convertToPrice(sl_range)
			if direction == tl.LONG:
				sl_price = round(entry_price + sl_range, 5)
			else:
				sl_price = round(entry_price - sl_range, 5)

			payload['order']['stopLossOnFill'] = {
				'price': str(sl_price)
			}

		if tp_price:
			payload['order']['takeProfitOnFill'] = {
				'price': str(round(tp_price, 5))
			}

		elif tp_range:
			tp_range = tl.convertToPrice(tp_range)
			if direction == tl.LONG:
				tp_price = round(entry_price + tp_range, 5)
			else:
				tp_price = round(entry_price - tp_range, 5)

			payload['order']['takeProfitOnFill'] = {
				'price': str(tp_price)
			}

		endpoint = f'/v3/accounts/{account_id}/orders'
		print(f'CREATE ORDER: {endpoint}, {payload}', flush=True)
		res = self._session.post(
			self._url + endpoint,
			headers=self._headers,
			data=json.dumps(payload)
		)

		result = {}
		status_code = res.status_code
		res = res.json()
		print(f'CREATE ORDER DONE: {status_code}, {res}', flush=True)

		return {'status': status_code, 'result': res}

	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price):

		payload_order_type = None
		if order['order_type'] == tl.LIMIT_ORDER:
			payload_order_type = 'LIMIT'
		elif order['order_type'] == tl.STOP_ORDER:
			payload_order_type = 'STOP'

		lotsize = self.convertToUnitSize(lotsize)
		if order['direction'] == tl.SHORT: 
			lotsize *= -1

		payload = {
			'order': {
				'price': str(entry_price),
				'instrument': order['product'],
				'units': str(int(lotsize)),
				'type': payload_order_type,
				'timeInForce': 'GTC',
				'positionFill': 'DEFAULT',
			}
		}

		if sl_price:
			payload['order']['stopLossOnFill'] = {
				'price': str(round(sl_price, 5))
			}

		if tp_price:
			payload['order']['takeProfitOnFill'] = {
				'price': str(round(tp_price, 5))
			}


		endpoint = f'/v3/accounts/{order["account_id"]}/orders/{order["order_id"]}'
		print(f'MODIFY ORDER: {endpoint}, {payload}', flush=True)
		res = self._session.put(
			self._url + endpoint,
			headers=self._headers,
			data=json.dumps(payload)
		)

		result = {}
		status_code = res.status_code
		res = res.json()
		print(f'MODIFY ORDER DONE: {status_code}, {res}', flush=True)

		return {'status': status_code, 'result': res}

	def deleteOrder(self, order):

		endpoint = f'/v3/accounts/{order["account_id"]}/orders/{order["order_id"]}/cancel'
		res = self._session.put(
			self._url + endpoint,
			headers=self._headers
		)

		result = {}
		status_code = res.status_code
		res = res.json()
		print(f'DELETE ORDER DONE: {status_code}, {res}', flush=True)

		return {'status': status_code, 'result': res}


	def convertToLotsize(self, size):
		return size / 100000


	def convertToUnitSize(self, size):
		return size * 100000


	# Live utilities
	def _reconnect(self):
		for sub in copy(self._subscriptions):
			sub.receive = False


	def _encode_params(self, params):
		return urlencode(dict([(k, v) for (k, v) in iter(params.items()) if v]))


	def _subscribe_chart_updates(self, msg_id, product):
		sub = Subscription(self, msg_id, Subscription.CHART, [product])
		self._subscriptions.append(sub)
		self._perform_chart_connection(sub)	


	def _perform_chart_connection(self, sub):
		endpoint = f'/v3/accounts/{self.accounts[0]}/pricing/stream'
		params = self._encode_params({
			'instruments': '%2C'.join(sub.args[0])
		})
		req = Request(f'{self._stream_url}{endpoint}?{params}', headers=self._headers)

		try:
			stream = urlopen(req, timeout=20)

			sub.setStream(stream)
			Thread(target=self._stream_price_updates, args=(sub,)).start()
		except Exception as e:
			time.sleep(1)
			print('[Oanda] Attempting price reconnect.', flush=True)
			Thread(target=self._perform_chart_connection, args=(sub,)).start()
			return


	def _stream_price_updates(self, sub):

		while sub.receive:
			try:
				message = sub.stream.readline().decode('utf-8').rstrip()
				if not message.strip():
					sub.receive = False
				else:
					sub.onUpdate(json.loads(message))

			except Exception as e:
				print(traceback.format_exc(), flush=True)
				sub.receive = False

		# Reconnect
		print('[Oanda] Price Updates Disconnected.', flush=True)
		self._perform_chart_connection(sub)


	def _subscribe_account_updates(self, msg_id, account_id):
		print(f'SUBSCRIBE ACCOUNT: {msg_id}, {account_id}', flush=True)
		sub = Subscription(self, msg_id, Subscription.ACCOUNT, account_id)
		self._subscriptions.append(sub)
		self._perform_account_connection(sub)


	def _perform_account_connection(self, sub):
		endpoint = f'/v3/accounts/{sub.args[0]}/transactions/stream'
		print(f'PERFORM CONNECTION: {endpoint}', flush=True)
		req = Request(f'{self._stream_url}{endpoint}', headers=self._headers)
		print(f'OANDA 1', flush=True)

		try:
			stream = urlopen(req, timeout=20)
			print(f'OANDA 2', flush=True)

			sub.setStream(stream)
			print(f'OANDA 3', flush=True)
			Thread(target=self._stream_account_update, args=(sub,)).start()
			print(f'OANDA 4', flush=True)
		except Exception as e:
			time.sleep(1)
			print('[Oanda] Attempting account reconnect.', flush=True)
			Thread(target=self._perform_account_connection, args=(sub,)).start()
			return


	def _stream_account_update(self, sub):
		print(f'accounts connected. {self._is_connected}', flush=True)
		if not self._is_connected:
			print('Send connected.', flush=True)
			self._is_connected = True
			self._last_update = time.time()

		sub.onUpdate(sub.args[0], { 'type': 'connected', 'item': True })

		while sub.receive:
			try:
				message = sub.stream.readline().decode('utf-8').rstrip()
				if not message.strip():
					sub.receive = False
				else:
					sub.onUpdate(sub.args[0], json.loads(message))

			except Exception as e:
				print(traceback.format_exc(), flush=True)
				sub.receive = False

		# Reconnect
		print('[Oanda] Account Updates Disconnected.', flush=True)

		if self._is_connected:
			self._is_connected = False

		self._perform_account_connection(sub)

