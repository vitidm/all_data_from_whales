import requests
import json
import time
import datetime
import pymysql

UNISWAP_V2_SUBGRAPH_URL = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
UNISWAP_V2_ROUTER_ADDRESS = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
def get_swaps_in_last_minutes(minutes):
    now_timestamp = int(time.time())
    past_timestamp = now_timestamp - 60 * minutes
    
    query = f"""
    {{
      swaps(first: 1000, where: {{timestamp_lte: "{now_timestamp}", sender: "{UNISWAP_V2_ROUTER_ADDRESS}", to_not_contains: "{UNISWAP_V2_ROUTER_ADDRESS}"}}, orderBy: timestamp, orderDirection: desc) {{
        id
        timestamp
        sender
        amount0In
        amount1In
        amount0Out
        amount1Out
        to
      }}
    }}
    """

    
    response = requests.post(UNISWAP_V2_SUBGRAPH_URL, json={'query': query})
    if response.status_code == 200:
        return json.loads(response.text)['data']['swaps']
    else:
        raise Exception(f"Error fetching swaps data: {response.text}")


def parse_swap_id(swap_id):
    parts = swap_id.split("-")
    
    return {
        "pair_address": parts[0]
    }

# Ajusta el número de minutos para filtrar los swaps en un rango de tiempo específico
minutes = 5
wait_time = 10  # Tiempo de espera en segundos entre iteraciones

processed_swaps = set()

def get_tx_hash_info(tx_hash):
    # Define la consulta para obtener el par de tokens
    query = f"""
    {{
    transactions(where: {{id: "{tx_hash}"}}) {{
        swaps {{
        pair {{
            token0 {{
            symbol
            name
            id
            }}
            token1 {{
            symbol
            name
            id
            }}
        }}
        }}
    }}
    }}
    """

    # Envía la consulta al subgrafo y obtiene la respuesta
    response = requests.post(UNISWAP_V2_SUBGRAPH_URL, json={'query': query})

    # Si la respuesta es correcta, extrae los tokens del par
    if response.status_code == 200:
        data = json.loads(response.text)['data']
        if data and 'transactions' in data and data['transactions']:
            swaps = data['transactions'][0]['swaps']
            if swaps:
                token1 = swaps[0]['pair']['token1']
                #print(f"El par de tokens del swap {tx_hash} es {token1['symbol']} | {token1['name']} | {token1['id']}")
                token_symbol = token1['symbol']
                token_name = token1['name']
                token_address = token1['id']
                return token_symbol, token_name, token_address
        else:
            print(f"No se encontró información para el swap con hash {tx_hash}")
    else:
        print(f"Error al obtener información del swap con hash {tx_hash}: {response.text}")

def insert_sql_info(timestamp, wallet_sender, eth_in, tx_hash, token_name, token_symbol, token_address):
    cnx = pymysql.connect(user='admin', password='E3I13bensamsung!',
                              host='35.222.125.208',
                              database='all_data_from_whales')
    
    # Realiza una consulta
    cursor = cnx.cursor()
    query = " INSERT INTO whale_data(date, wallet_sender, eth_buy, tx_hash, token_name, token_symbol, token_address) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE tx_hash = %s"
    data = (timestamp, wallet_sender, eth_in, tx_hash, token_name, token_symbol, token_address, tx_hash)

    cursor.execute(query, data)
    cnx.commit()

    cursor.close()
    cnx.close()

while True:
    print(f"Fetching swaps in the last {minutes} minutes...")
    swaps = get_swaps_in_last_minutes(minutes)

    for swap in swaps:
        swap_id = swap['id']

        if swap_id not in processed_swaps:  # Verificar si el swap ya ha sido procesado
            processed_swaps.add(swap_id)
            # Filtrar solo las compras (to == UNISWAP_V2_ROUTER_ADDRESS)
            # if swap['to'] == UNISWAP_V2_ROUTER_ADDRESS:
                # if float(swap['amount0Out']) > 0.1:
            swap_info = parse_swap_id(swap['id'])
            token_symbol, token_name, token_address = get_tx_hash_info(swap_info['pair_address'])
            if token_symbol != "WETH" and token_symbol != "USDT" and token_symbol != "UNI-V2" and token_symbol != "GETH" and token_symbol != "USDC":
                swap_id = swap['id']
                tx_hash = swap_info['pair_address']
                timestamp = datetime.datetime.fromtimestamp(int(swap['timestamp']))- datetime.timedelta(hours=2)
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                wallet_sender = swap['to']
                eth_in = float(swap['amount0In'])
                tokens_out = swap['amount0Out']
                if eth_in >= 0.3 and eth_in <= 20.0:
                    print(swap)
                    print(f"Swap ID: {swap_id}")
                    print(f"  Tx Hash: {tx_hash}")
                    print(f"  TOKEN INFO: \n  - Token Symbol: {token_symbol}\n  - Token Name: {token_name}\n  - Token Address: {token_address}")
                    print(f"  Timestamp: {timestamp}")
                    print(f"  Wallet: {wallet_sender}")
                    print(f"  Amount ETH In: {eth_in}")
                    print(f"  Amount Tokens Out: {tokens_out}")
                    print()
                    insert_sql_info(timestamp, wallet_sender, eth_in, tx_hash, token_name, token_symbol, token_address)

    time.sleep(wait_time)