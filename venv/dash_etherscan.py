import requests
import pandas as pd
import os
import schedule
import time
import threading
from datetime import datetime, timedelta
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
import plotly.express as px

# API Key do Etherscan
api_key = 'ST45D2FATG77Z99AE9D4DX3TFW8A5WFGN8'

# Nome do arquivo Excel
excel_file = 'flash_loan_data.xlsx'

# Arquivo de texto para armazenar a última data processada
date_file = 'last_processed_date.txt'

# Data padrão inicial se não houver data armazenada
default_start_date = datetime(2019, 2, 5)

# Variável para rastrear o número de requisições
request_count = 0
MAX_REQUESTS = 100  # Limite de requisições por sessão, ajustável
INCREMENT_DAYS = 30  # Configuração para quantos dias incrementar a cada execução

# Lista de contratos de flash loans conhecidos
contracts = [
    '0x398ec7346dcd622edc5ae82352f02be94c62d119',  # Aave
    '0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e',  # dYdX
    '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',  # Uniswap
    '0x9b6443b0fB9C241A7fdAC375595cEa13e6B7807A',  # Exemplo adicional
    '0x0000000000095413afC295d19EDeb1Ad7B71c952',  # Exemplo adicional
    '0xAf615B61448691fC3E4c61AE4F015d6e77b6CCa8',  # Exemplo adicional
    '0xc01b1979e2244Dc94e67891df0Af4F7885e57fD4'  # Exemplo adicional
]


# Função para obter o número de bloco a partir de uma data
def get_block_by_date(date):
    global request_count
    if request_count >= MAX_REQUESTS:
        print("Limite de requisições atingido!")
        return None
    timestamp = int(date.timestamp())
    url = f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={timestamp}&closest=before&apikey={api_key}'
    response = requests.get(url)
    request_count += 1
    data = response.json()

    if data['status'] == '1':
        return int(data['result'])
    else:
        print(f"Erro ao buscar bloco para data: {date}. Detalhes: {data['message']}")
        return None


# Função para carregar a última data do arquivo de texto
def get_last_processed_date():
    if os.path.exists(date_file):
        with open(date_file, 'r') as file:
            last_date = file.read().strip()
            print(f"Data recuperada do arquivo: {last_date}")
            return datetime.strptime(last_date, '%Y-%m-%d')
    print("Nenhuma data encontrada no arquivo, utilizando data padrão.")
    return default_start_date


# Função para salvar a última data no arquivo de texto (cria o arquivo se não existir)
def save_last_processed_date(date):
    with open(date_file, 'w') as file:
        file.write(date.strftime('%Y-%m-%d'))
    print(f"Data {date.strftime('%Y-%m-%d')} salva no arquivo.")


# Função para obter transações entre blocos
def get_transactions(contract_address, start_block, end_block):
    global request_count
    if request_count >= MAX_REQUESTS:
        print("Limite de requisições atingido!")
        return []

    url = f'https://api.etherscan.io/api?module=account&action=txlist&address={contract_address}&startblock={start_block}&endblock={end_block}&sort=asc&apikey={api_key}'
    response = requests.get(url)
    request_count += 1
    data = response.json()

    if data['status'] == '1' and data['message'] == 'OK':
        return data['result']
    else:
        print(
            f"Erro ao buscar transações entre blocos {start_block} e {end_block} para o contrato {contract_address}. Detalhes: {data['message']}")
        return []


# Função para carregar dados existentes do Excel
def load_existing_data():
    if os.path.exists(excel_file):
        return pd.read_excel(excel_file)
    else:
        return pd.DataFrame(columns=['blockHash'])  # Certificar que a coluna existe


# Função principal para realizar a consulta por data incremental
def update_excel(start_date=None, end_date=None):
    global request_count

    if start_date is None:
        start_date = get_last_processed_date()

    if end_date is None:
        end_date = start_date + timedelta(days=INCREMENT_DAYS)

    print(f"Iniciando a busca de {start_date} até {end_date}")

    existing_data = load_existing_data()
    all_new_transactions = []

    try:
        for contract in contracts:
            if request_count >= MAX_REQUESTS:
                break

            print(f"Processando contrato: {contract}")

            # Obter o bloco inicial e final com base nas datas
            start_block, valid_start_date = get_block_by_date(start_date), start_date
            end_block, valid_end_date = get_block_by_date(end_date), end_date

            if not start_block or not end_block:
                print(
                    f"Falha ao obter blocos para o intervalo {valid_start_date} a {valid_end_date}. Pulando este contrato.")
                continue

            transactions = get_transactions(contract, start_block, end_block)
            if transactions:
                for tx in transactions:
                    if existing_data.empty or tx['blockHash'] not in existing_data['blockHash'].values:
                        all_new_transactions.append(tx)

        if all_new_transactions:
            new_data_df = pd.DataFrame(all_new_transactions)

            # Converter timestamps e outros campos
            new_data_df['timeStamp'] = pd.to_numeric(new_data_df['timeStamp'])
            new_data_df['timeStamp'] = pd.to_datetime(new_data_df['timeStamp'], unit='s')
            new_data_df['date'] = new_data_df['timeStamp'].dt.date  # Criar a coluna 'date'
            new_data_df['value'] = new_data_df['value'].astype(float) / 1e18  # Convertendo de wei para ether

            # Combinar com os dados existentes
            combined_data = pd.concat([existing_data, new_data_df], ignore_index=True)

            # Salvar no Excel
            combined_data.to_excel(excel_file, index=False)
            print(f"{len(new_data_df)} novas transações adicionadas ao Excel.")

    except Exception as e:
        print(f"Ocorreu um erro durante o processamento: {e}")

    finally:
        # Atualizar e salvar a última data processada
        save_last_processed_date(end_date)


# Função de agendamento
def scheduled_task():
    global request_count
    request_count = 0  # Resetar o contador de requisições
    update_excel()
    print(f"Dados atualizados com sucesso.")


# Executar a tarefa imediatamente ao iniciar o script
scheduled_task()

# Configurar o agendamento para cada 10 minutos
schedule.every(20).seconds.do(scheduled_task)


# Função para carregar os dados do Excel e criar o dashboard
def create_dashboard():
    df = pd.read_excel(excel_file)

    if 'timeStamp' in df.columns:
        df['timeStamp'] = pd.to_datetime(df['timeStamp'], unit='s')
        df['date'] = df['timeStamp'].dt.date

    app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    app.layout = html.Div([
        html.H1("Flash Loan Dashboard"),
        dcc.Tabs([
            dcc.Tab(label='Número de Transações por Dia', children=[
                dcc.Graph(
                    id='transacoes-por-dia',
                    figure=px.bar(df.groupby('date').size().reset_index(name='counts'), x='date', y='counts',
                                  title='Número de Transações por Dia')
                )
            ]),
            dcc.Tab(label='Movimentação Financeira por Dia', children=[
                dcc.Graph(
                    id='movimentacao-por-dia',
                    figure=px.line(df.groupby('date')['value'].sum().reset_index(), x='date', y='value',
                                   title='Movimentação Financeira por Dia (ETH)')
                )
            ])
        ])
    ])
    app.run_server(debug=False)

    # Função para rodar o Dash em uma thread separada


def run_dash():
    create_dashboard()

    # Criar e iniciar a thread para o Dash


dash_thread = threading.Thread(target=run_dash)
dash_thread.start()

# Loop para manter o agendamento em execução
while True:
    schedule.run_pending()
    time.sleep(1)


