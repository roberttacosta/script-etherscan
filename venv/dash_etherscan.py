import requests
import pandas as pd
import os
import schedule
import time
import threading
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
import plotly.express as px

# api key etherscan
api_key = 'apikey'

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

# Nome do arquivo Excel
excel_file = 'flash_loan_data.xlsx'

# endpoint transações de um contrato
def get_transactions(contract_address):
    url = f'https://api.etherscan.io/api?module=account&action=txlist&address={contract_address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    if data['status'] == '1' and data['message'] == 'OK':
        return data['result']
    else:
        return []


# Função para carregar dados existentes do Excel
def load_existing_data():
    if os.path.exists(excel_file):
        df = pd.read_excel(excel_file)
        return df
    else:
        return pd.DataFrame(columns=['blockHash'])  # Certificar que a coluna existe


# Função principal para executar o script e atualizar o Excel
def update_excel(start_date=None, end_date=None):
    existing_data = load_existing_data()

    all_new_transactions = []
    for contract in contracts:
        print(f"Processando contrato: {contract}")
        transactions = get_transactions(contract)
        if transactions:
            for tx in transactions:
                # Verifique se o blockHash já existe
                if existing_data.empty or tx['blockHash'] not in existing_data['blockHash'].values:
                    all_new_transactions.append(tx)

    if all_new_transactions:
        new_data_df = pd.DataFrame(all_new_transactions)

        # Converter timestamps e outros campos
        new_data_df['timeStamp'] = pd.to_numeric(new_data_df['timeStamp'])
        new_data_df['timeStamp'] = pd.to_datetime(new_data_df['timeStamp'], unit='s')
        new_data_df['date'] = new_data_df['timeStamp'].dt.date  # Criar a coluna 'date'
        new_data_df['value'] = new_data_df['value'].astype(float) / 1e18  # Convertendo de wei para ether

        # Filtro por data
        if start_date:
            new_data_df = new_data_df[new_data_df['timeStamp'] >= pd.to_datetime(start_date)]
        if end_date:
            new_data_df = new_data_df[new_data_df['timeStamp'] <= pd.to_datetime(end_date)]

        # Combinar com os dados existentes
        combined_data = pd.concat([existing_data, new_data_df], ignore_index=True)

        # Salvar no Excel
        combined_data.to_excel(excel_file, index=False)
        print(f"{len(new_data_df)} novas transações adicionadas ao Excel.")
    else:
        print("Nenhuma nova transação foi encontrada.")


# Função para carregar os dados do Excel e criar o dashboard
def create_dashboard():
    # Carregar dados do Excel
    df = pd.read_excel(excel_file)

    # Garantir que a coluna 'date' exista e esteja no formato correto
    if 'timeStamp' in df.columns:
        df['timeStamp'] = pd.to_datetime(df['timeStamp'], unit='s')
        df['date'] = df['timeStamp'].dt.date

    # Criação do dashboard
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

    app.run_server(debug=False)  # Desativar o modo de depuração ao executar em uma thread secundária


# Função de agendamento
def scheduled_task():
    update_excel(start_date='2024-01-01', end_date='2024-08-01')
    print("Dados atualizados com sucesso.")


# Executar a tarefa imediatamente ao iniciar o script
scheduled_task()

# Configurar o agendamento para cada 10 minutos (ajuste conforme necessário)
schedule.every(10).minutes.do(scheduled_task)


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
