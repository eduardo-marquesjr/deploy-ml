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
from tratamento import get_tabela_cotas
from tratamento2 import trata_e_roda
warnings.filterwarnings('ignore') 

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_AS_ASCII'] = False
app.secret_key = 'ccm'

dados_nomes, dados_produtos, dados_precos = trata_e_roda()

cotas_fundos_cvm_abril_19 = get_tabela_cotas("201904") 
print("201904")
cotas_fundos_cvm_maio_19 = get_tabela_cotas("201905")  
print("201905")
cotas_fundos_cvm_junho_19 = get_tabela_cotas("201906") 
print("201906")
cotas_fundos_cvm_julho_19 = get_tabela_cotas("201907") 
print("201907")
cotas_fundos_cvm_agosto_19 = get_tabela_cotas("201908") 
print("201908")
cotas_fundos_cvm_setembro_19 = get_tabela_cotas("201909") 
print("201909")
cotas_fundos_cvm_outubro_19 = get_tabela_cotas("201910") 
print("201910")
cotas_fundos_cvm_novembro_19 = get_tabela_cotas("201911") 
print("201911")
cotas_fundos_cvm_dezembro_19 = get_tabela_cotas("201912") 
print("201912")
cotas_fundos_cvm_janeiro_20 = get_tabela_cotas("202001") 
print("202001")
cotas_fundos_cvm_fevereiro_20 = get_tabela_cotas("202002") 
print("202002")
cotas_fundos_cvm_marco_20 = get_tabela_cotas("202003") 
print("202003")
cotas_fundos_cvm_abril_20 = get_tabela_cotas("202004") 
print("202004")
cotas_fundos_cvm_maio_20 = get_tabela_cotas("202005")  
print("202005")
cotas_fundos_cvm_junho_20 = get_tabela_cotas("202006")  
print("202006")
cotas_fundos_cvm_julho_20 = get_tabela_cotas("202007")  
print("202007")
cotas_fundos_cvm_agosto_20 = get_tabela_cotas("202008")  
print("202008")
cotas_fundos_cvm_setembro_20 = get_tabela_cotas("202009")  
print("202009")
cotas_fundos_cvm_outubro_20 = get_tabela_cotas("202010")  
print("202010")
cotas_fundos_cvm_novembro_20 = get_tabela_cotas("202011")  
print("202011")
cotas_fundos_cvm_dezembro_20 = get_tabela_cotas("202012")
print("202012")
cotas_fundos_cvm_janeiro_21 = get_tabela_cotas("202101")
print("202101")
cotas_fundos_cvm_fevereiro_21 = get_tabela_cotas("202102") 
print("202102")
cotas_fundos_cvm_marco_21 = get_tabela_cotas("202103")
print("202103")
cotas_fundos_cvm_abril_21 = get_tabela_cotas("202104")
print("202104")
cotas_fundos_cvm_maio_21 = get_tabela_cotas("202105") 
print("202105")
cotas_fundos_cvm_junho_21 = get_tabela_cotas("202106")
print("202106")
cotas_fundos_cvm_julho_21 = get_tabela_cotas("202107")   
print("202107")

dados_fundos = pd.DataFrame()
dados_fundos = pd.concat([cotas_fundos_cvm_abril_19, cotas_fundos_cvm_maio_19, cotas_fundos_cvm_junho_19, 
cotas_fundos_cvm_julho_19, cotas_fundos_cvm_agosto_19, cotas_fundos_cvm_setembro_19, 
cotas_fundos_cvm_outubro_19, cotas_fundos_cvm_novembro_19, cotas_fundos_cvm_dezembro_19,
cotas_fundos_cvm_janeiro_20, cotas_fundos_cvm_fevereiro_20, cotas_fundos_cvm_marco_20,
cotas_fundos_cvm_abril_20, cotas_fundos_cvm_maio_20, cotas_fundos_cvm_junho_20,
cotas_fundos_cvm_julho_20, cotas_fundos_cvm_agosto_20, cotas_fundos_cvm_setembro_20,
cotas_fundos_cvm_outubro_20, cotas_fundos_cvm_novembro_20, cotas_fundos_cvm_dezembro_20,
cotas_fundos_cvm_janeiro_21, cotas_fundos_cvm_fevereiro_21, cotas_fundos_cvm_marco_21,
cotas_fundos_cvm_abril_21, cotas_fundos_cvm_maio_21, cotas_fundos_cvm_junho_21,
cotas_fundos_cvm_julho_21]) 

dados_fundos = dados_fundos.fillna(method = 'bfill') 
dados_precos.reset_index(inplace = True)
dados_fundos.reset_index(inplace = True) 
dados_fundos.rename(columns = {'index': 'Date_p'}, inplace = True) 

dados_fundos['Date_p'] = pd.to_datetime(dados_fundos['Date_p'], format = '%Y-%m-%d') 
dados_precos['Date'] = pd.to_datetime(dados_precos['Date'], format = '%Y-%m-%d')
dados_precos2 = dados_precos[dados_precos.Date >= '2019-04-01'] 
dados_precos2.reset_index(drop = True, inplace = True)   
dados_fundos2 = dados_fundos.copy() 

dados_precos_fundos = pd.concat([dados_precos2.copy(), dados_fundos2.copy()], axis = 1)
dados_precos_fundos.set_index('Date', inplace = True) 
dados_precos_fundos.dropna(inplace = True)

dados_precos_fundos.index = dados_precos_fundos.index.map(str)  
dados_precos_fundos.reset_index(inplace = True)
dados_precos_fundos['Date'] = pd.to_datetime(dados_precos_fundos['Date'], format = "%Y-%m-%d")
dados_precos_fundos.drop('Date_p', inplace = True, axis = 1)  
dados_precos_fundos.set_index('Date', inplace = True) 

retorno = dados_precos_fundos.pct_change()
retorno = retorno.iloc[1:] 
retorno_anual = retorno.mean() * 250
cov_diaria = retorno.cov() 
cov_anual = cov_diaria * 250

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
