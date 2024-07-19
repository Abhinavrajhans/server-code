import redis
import time
import json, glob, numpy as np
import direct_redis
import pandas as pd
import datetime

r = direct_redis.DirectRedis()

clients = r.hgetall('live_clients')


def get_time():
    return datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S.%f')

def get_index_ltp():
    ltp_dict = {}
    for i in ['NIFTYSPOT', 'BANKNIFTYSPOT', 'FINNIFTYSPOT', 'MIDCPNIFTYSPOT', 'SENSEXSPOT']:
        ltp = r.get(f'ltp.{i}')
        if ltp is not None:
            ltp_dict[i] = ltp
            
    return ltp_dict

def get_client_margin(client):
    margin = r.hget('curr_client_margin', client)
    if margin is None:
        return 0
    else:
        return round(margin)

def get_client_var(client):
    var = r.hget('curr_client_var', client)
    if var is None:
        return 0
    else:
        return abs(var)

def get_client_rms_df(client):
    rms_df = r.hget('live_client_rms_df', client)
    return rms_df


def get_client_pos(client, exchangeTOsymbol):
    pos = r.hget('live_user_positions', client)
    return {exchangeTOsymbol[k]:v for k, v in pos.items()}

def get_history(date, client):
    f = glob.glob(f'../blackbox/{date}/{client}/trade_book.csv')
    tb = pd.read_csv(f)

    if len(tb) == 0:
        return 0
    else:
        df = df[df.OrderStatus == 'Filled']
        df['OrderQuantity'] = np.where(df['OrderSide'] == 'BUY', df['OrderQuantity'], -df['OrderQuantity'])
        df['value'] = df['OrderQuantity'] * df['OrderAverageTradedPrice']
        pnl = df['value'].sum()
        return pnl

def get_net_qty(client):
    pos = r.hget('live_user_positions', client)
    return sum([x for x in pos.values()])

def get_open_qty(client):
    pos = r.hget('live_user_positions', client)
    return sum([abs(x) for x in pos.values()])

def get_idealMTM(client):
    return r.hgetall('curr_client_ideal_mtm')[client]

def get_ideal_client_mtm(client):
    mtm_list=r.hgetall(f'live_client_ideal_mtm.{client}')
    last_key, last_value = list(mtm_list.items())[-1]
    return last_value

def get_actual_client_MTM(client):
    mtm_list=r.hgetall(f'live_client_mtm.{client}')
    last_key, last_value = list(mtm_list.items())[-1]
    return last_value

def getRejectedOrders(client):
    rejected_orders = r.hget('rejected_orders', client)
    if rejected_orders is None:
        rejected_orders = 0
    return rejected_orders

def getPendingOrders(client):
    pending_orders = r.hget('pending_orders', client)
    if pending_orders is None:
        pending_orders = 0
    return pending_orders

def get_basketData(live_weights):
    
    ltp_df = []
    
    for basket in live_weights:
        ltp_dict = r.hgetall(f'live_mtm.{basket}')
        
        if ltp_dict is not None:
            ltp = pd.Series(ltp_dict)
            ltp = ltp[ltp != 0]
            ltp = ltp.sort_index()
            ltp.name = basket
            ltp_df.append({basket : ltp.to_dict()})

    return ltp_df

def get_live_positions(live_weights):
    system_timestamp = datetime.datetime.now()
    pnl_str = f'TIME : {system_timestamp}  | '
    total_trades = []
    for basket, wts in live_weights.items():
        strats = r.hmget('live_strats', list(wts.index))
        if strats is not None:
            all_trades = sum([x.trades for x in strats if x is not None], [])
            if len(all_trades) > 0:
                total_trades.extend(all_trades)
                all_trades = pd.DataFrame(all_trades)
                all_trades['friction'] = all_trades['value'].abs()*0.01
                all_trades['net_value'] = all_trades['value']-all_trades['friction']        
                pnl_str += f'{basket.upper()} : {all_trades["net_value"].sum().round()} | '
    return (pnl_str)

def read_pulse_updates():
    pulse_updates = r.get('rms_heartbeat')
    return pulse_updates


def get_client_live_trade_book(client):
    exchangeTOsymbol = r.get('exchangeTOsymbol')
    tb = r.hget('live_user_tb', client)
    if len(tb) > 0:
        tb = pd.DataFrame(tb)
        tb = tb[tb.OrderStatus == 'Filled']
        # tb['OrderGeneratedDateTime'] = pd.to_datetime(tb['OrderGeneratedDateTime'], format='%d-%m-%Y %H:%M:%S')
        tb['ExchangeInstrumentID'] = tb['ExchangeInstrumentID'].map(exchangeTOsymbol)
        temp_tb = tb[['OrderGeneratedDateTime', 'ExchangeTransactTime', 'ExchangeInstrumentID', 'OrderAverageTradedPrice', 'OrderSide', 'OrderQuantity']]
    
        return temp_tb.to_dict('records')

def get_client_live_order_book(client):
    exchangeTOsymbol = r.get('exchangeTOsymbol')
    tb = r.hget('live_user_ob', client)
    if len(tb) > 0:
        tb = pd.DataFrame(tb)
        # tb['OrderGeneratedDateTime'] = pd.to_datetime(tb['OrderGeneratedDateTime'], format='%d-%m-%Y %H:%M:%S')
        tb['ExchangeInstrumentID'] = tb['ExchangeInstrumentID'].map(exchangeTOsymbol)
        temp_tb = tb[['OrderGeneratedDateTime', 'ExchangeTransactTime', 'ExchangeInstrumentID', 'OrderAverageTradedPrice', 'OrderSide', 'LeavesQuantity', 'OrderQuantity', 'OrderStatus', 'CancelRejectReason']]
        return temp_tb.to_dict('records')

def get_last_value_for_strategy_mtm_chart_data(str):
    data = r.hgetall('live.mtm_' + str)
    df = pd.DataFrame(list(data.items()), columns=['Timestamp', 'Value'])
    
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df.set_index('Timestamp', inplace=True)
    df.sort_values(by='Timestamp', inplace=True)
    
    today = datetime.datetime.now().date()
    df_today = df[df.index.date == today]
    
    if not df_today.empty:
        last_timestamp = df_today.index[-1].strftime('%Y-%m-%d %H:%M:%S')
        last_value = float(df_today['Value'].iloc[-1])
        return {last_timestamp: last_value}
    else:
        return {}





def get_strategy_mtm_chart_data(live_weights):
    keys_in_non_directional = list(live_weights['non_directional'].keys())
    keys_in_directional = list(live_weights['directional'].keys())
    # keys_in_nikbuy = list(live_weights['nikbuy'].keys())

    key_value_pairs_non_directional = [{key: get_last_value_for_strategy_mtm_chart_data(key)} for key in keys_in_non_directional ]
    key_value_pairs_directional = [{key: get_last_value_for_strategy_mtm_chart_data(key)} for key in keys_in_directional ]
    # key_value_pairs_nikbuy = [{key: get_last_value_for_strategy_mtm_chart_data(key)} for key in keys_in_nikbuy ]
    return {
        'non_directional': key_value_pairs_non_directional,
        'directional': key_value_pairs_directional,
        # 'nikbuy': key_value_pairs_nikbuy,
    }


def publish_data():
    # Connect to Redis
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

    # Publish numbers 1 to 10
    if clients is not None:
        while True:
            system_timestamp = datetime.datetime.now()

            exchangeTOsymbol = r.get('exchangeTOsymbol')
            clients_obj = [
                {
                    'name': client,
                    'ideal_MTM': round(get_idealMTM(client),2),
                    'MTM': round(get_actual_client_MTM(client),2),
                    "MTMTable":r.hgetall(f'live_client_mtm.{client}'),
                    'ideal_MTMTable':r.hgetall(f'live_client_ideal_mtm.{client}'),
                    'Rejected_orders': getRejectedOrders(client), 
                    'Pending_orders': getPendingOrders(client),
                    'OpenQuantity':get_open_qty(client),
                    "NetQuantity":get_net_qty(client),
                    "Live_Trade_Book":get_client_live_trade_book(client),
                    "Live_Order_Book":get_client_live_order_book(client),
                    "Live_Client_Positions":get_client_pos(client, exchangeTOsymbol),
                    "Live_Client_RMS_df":get_client_rms_df(client),
                    "Live_Client_Margin":get_client_margin(client),
                    "Live_Client_Var":get_client_var(client),
                }
                for client in clients
            ]
            live_weights = r.hgetall('live_weights')
            strategy_mtm_chart_data=get_strategy_mtm_chart_data(live_weights)
            Basket_data=get_basketData(live_weights)
            client_Object={ "client_data":clients_obj }
            basket_Object={"basket_data":Basket_data}
            strategy_mtm_chart_data_Object={"strategy_mtm_chart_data":strategy_mtm_chart_data}
            connection_Object={
                'time':get_time(),
                "live_index":get_index_ltp(),
                'pulse':read_pulse_updates()
            }
            client_object_json_message = json.dumps(client_Object)
            basket_object_json_message = json.dumps(basket_Object)
            connection_object_json_message = json.dumps(connection_Object)
            strategy_mtm_chart_data_json_message = json.dumps(strategy_mtm_chart_data_Object)


            # Publish the message
            redis_client.publish('client_dashboard_data', client_object_json_message)
            redis_client.publish('basket_dashboard_data', basket_object_json_message)
            redis_client.publish('connection_dashboard_data', connection_object_json_message)
            redis_client.publish('strategy_mtm_chart_data', strategy_mtm_chart_data_json_message)
            
            # Wait for 1 second before publishing the next message
            print(system_timestamp, sep='', end='\r', flush=True)  
            time.sleep(0.2)
          

if __name__ == "__main__":
    publish_data()
