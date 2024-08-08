import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objs as go

# api key etherscan
api_key = 'apikey'

contracts = [
    '0x398ec7346dcd622edc5ae82352f02be94c62d119',
    '0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e',
    '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
    '0x9b6443b0fB9C241A7fdAC375595cEa13e6B7807A',
    '0x0000000000095413afC295d19EDeb1Ad7B71c952',
    '0xAf615B61448691fC3E4c61AE4F015d6e77b6CCa8',
    '0xc01b1979e2244Dc94e67891df0Af4F7885e57fD4'
]


# endpoint transações de um contrato
def get_transactions(contract_address):
    url = f'https://api.etherscan.io/api?module=account&action=txlist&address={contract_address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    if data['status'] == '1' and data['message'] == 'OK':
        return data['result']
    else:
        return []


# endpoint detalhes do contrato
def get_contract_details(contract_address):
    url = f'https://api.etherscan.io/api?module=contract&action=getsourcecode&address={contract_address}&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    return data['result'][0] if data['status'] == '1' and data['message'] == 'OK' else {}



def collect_data(start_date=None, end_date=None):
    all_transactions = []
    for contract in contracts:
        print(f"Processando contrato: {contract}")
        transactions = get_transactions(contract)
        contract_details = get_contract_details(contract)
        if transactions:
            print(f"Primeiras transações de {contract}: {transactions[:2]}")
        all_transactions.extend(transactions)
        print(f"Detalhes do Contrato: {contract_details}")

    df = pd.DataFrame(all_transactions)

    # conversao timestamps
    df['timeStamp'] = pd.to_numeric(df['timeStamp'])
    df['timeStamp'] = pd.to_datetime(df['timeStamp'], unit='s')
    df['date'] = df['timeStamp'].dt.date

    # filtro por data
    if start_date:
        df = df[df['timeStamp'] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df['timeStamp'] <= pd.to_datetime(end_date)]

    return df


# data início e término
start_date = '2024-01-01'
end_date = '2024-08-01'

df = collect_data(start_date, end_date)

# Log
print(df.head())

# Verificar se 'value' está no DataFrame
if 'value' in df.columns:
    df['value'] = df['value'].astype(float) / 1e18  # Convertendo de wei para ether
else:
    raise KeyError("'value' não encontrado no DataFrame. Verifique a resposta da API do Etherscan.")

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

if __name__ == '__main__':
    app.run_server(debug=True)
