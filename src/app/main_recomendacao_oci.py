# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, redirect, url_for, jsonify
import pandas as pd
import yfinance as yf
import datetime as dt
import numpy as np
import os
import datetime as dt
import math
from sklearn.cluster import KMeans
from sklearn import metrics 
import warnings
import json
import mysql.connector
import time 
from tratamento import trata_e_roda
warnings.filterwarnings('ignore') 

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_AS_ASCII'] = False
app.secret_key = 'ccm'

dados_nomes, dados_produtos, dados_precos_fundos, retorno_anual, cov_anual = trata_e_roda()

@app.route('/home')
def home():
    return 'Olá, CCMers'

@app.route('/contas')
def contas():
    dados_contas = []
    dados_contas_final = {} 
    dados_contas_str = dados_nomes.Conta
    dados_contas_str = dados_contas_str.astype('O')
    for k in range(len(dados_nomes.Conta.unique())):
        dados_contas.append(dados_contas_str.unique()[k])
    dados_contas_final['Contas'] = dados_contas
    # dados_contas_final = {'Contas' : dados_contas_final}
    return jsonify(Data=dados_contas_final) 

@app.route('/recomenda/<int:conta>') 
def recomenda(conta): 
    dados_filtrado = dados_nomes[['Conta','Tipo','Profissao', 'Estado_Civil', 'Estado', 'Perfil_do_Cliente',
                                 'Tipo_Investidor', 'Faixa_Cliente', 'Idade']][dados_nomes['Conta'] == conta]
    dados_filtrado2 = dados_produtos[['Conta', 'Mercado', 'Produto', 'Segmento', 'Ativo', 
                                         'Categoria']][dados_produtos['Conta'] == conta]
    localiza = dados_nomes[dados_nomes['Conta'] == conta] 
    colunas = dados_filtrado.columns.values  
    colunas2 = dados_filtrado2.columns.values 
    cluster = localiza.Clusters[0:1].values[0] 
    recomendacoes = dados_nomes[dados_nomes.Clusters == cluster] 
    recomendacoes = recomendacoes['Categoria-Segmento'][recomendacoes.Clusters == cluster].unique()
    recomendacoes = [recomendacoes[i] for i in range(len(recomendacoes))] 
    tamanho_recomendacao = len(recomendacoes) 
    
    produtos_carteira = np.unique(dados_produtos[['Produto']][(dados_produtos['Conta'] == conta) 
            & ((dados_produtos['Mercado'] == 'Derivativos') | (dados_produtos['Mercado'] == 'Fundos')
            (dados_produtos['Mercado'] == 'Renda Variável'))].values)
    produtos_carteira = produtos_carteira + '.SA'
    produtos_carteira1 = [produtos_carteira[i] for i in range(len(produtos_carteira)) if produtos_carteira[i] in
                          dados_precos_fundos.columns.values] 
    port_returns = []
    port_volatility = [] 
    stock_weights = []
    lista_sharpe_ratio = []

    num_assets = len(produtos_carteira1)
    num_portfolios = 1000

    for single_portfolio in range(num_portfolios):
        pesos_carteira = np.random.random(num_assets)
        pesos_carteira /= np.sum(pesos_carteira)
        returns = np.dot(pesos_carteira, retorno_anual.loc[produtos_carteira1])
        volatility = np.sqrt(np.dot(pesos_carteira.T, np.dot(cov_anual[produtos_carteira1].loc[produtos_carteira1],pesos_carteira)))
        port_returns.append(returns) 
        port_volatility.append(volatility)
        stock_weights.append(pesos_carteira) 
        sharpe_ratio = returns / volatility
        lista_sharpe_ratio.append(sharpe_ratio) 

    portfolio = {'Retornos' : port_returns, 'Volatilidade' : port_volatility, 'Sharpe' : lista_sharpe_ratio}

    for counter, symbol in enumerate(produtos_carteira1):
        portfolio[symbol + ' peso'] = [pesos_carteira[counter] for pesos_carteira in stock_weights]

    df = pd.DataFrame(portfolio) 
                          
    maior_sharpe = df['Sharpe'].max()
    carteira_maior_sharpe = df.loc[df['Sharpe'] == maior_sharpe] 
    carteira_maior_sharpe = round(carteira_maior_sharpe, 3)
    colunas3 = carteira_maior_sharpe.columns.values
    
    json_cliente = {}
    for n in range(len(dados_filtrado.columns.values)):
        json_cliente[dados_filtrado.columns.values[n]] = dados_filtrado[0:1].values[0][n] 

    json_carteira = {}
    lista_conta = [dados_filtrado2[m:m+1].values[0][0] for m in range(dados_filtrado2.shape[0])]
    lista_mercado = [dados_filtrado2[m:m+1].values[0][1] for m in range(dados_filtrado2.shape[0])]
    lista_produto = [dados_filtrado2[m:m+1].values[0][2] for m in range(dados_filtrado2.shape[0])]
    lista_segmento = [dados_filtrado2[m:m+1].values[0][3] for m in range(dados_filtrado2.shape[0])]
    lista_ativo = [dados_filtrado2[m:m+1].values[0][4] for m in range(dados_filtrado2.shape[0])]
    lista_categoria = [dados_filtrado2[m:m+1].values[0][5] for m in range(dados_filtrado2.shape[0])]
    json_carteira[dados_filtrado2.columns.values[0]] = lista_conta
    json_carteira[dados_filtrado2.columns.values[1]] = lista_mercado
    json_carteira[dados_filtrado2.columns.values[2]] = lista_produto
    json_carteira[dados_filtrado2.columns.values[3]] = lista_segmento
    json_carteira[dados_filtrado2.columns.values[4]] = lista_ativo
    json_carteira[dados_filtrado2.columns.values[5]] = lista_categoria
    
    json_balanco = {}
    if len(carteira_maior_sharpe) > 0:
        for n in range(len(carteira_maior_sharpe.columns.values)):
            json_balanco[carteira_maior_sharpe.columns.values[n]] = carteira_maior_sharpe[0:1].values[0][n] 
    else:
        json_balanco = {}

    json_recomendacoes = {'Recomendacoes' : recomendacoes}

    json_final = {
        'Cliente' : json_cliente,
        'Carteira' : json_carteira,
        'Balanceamento' : json_balanco,
        'Recomendacoes' : json_recomendacoes
    }

    return jsonify(Data=json_final)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=100)   
