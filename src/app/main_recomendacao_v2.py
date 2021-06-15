# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, redirect, session, flash, url_for, jsonify
from flask_basicauth import BasicAuth
import pandas as pd
import datetime as dt
import numpy as np
import os
from pandas_datareader import data as pdr
from textblob import TextBlob
import pickle
import datetime as dt
import math
from sklearn.cluster import KMeans
from sklearn import metrics 
import warnings
warnings.filterwarnings('ignore') 

def trata_e_roda():
    # -*- coding: utf-8 -*-
    base_btg_produtos1 = 'SELECT * FROM `pristine-bonito-301012.ccmteste.pos`'
    base_btg_produtos = pd.read_gbq(base_btg_produtos1) 
    base_btg_produtos.rename(columns = {'CONTA': 'Conta', 'MERCADO': 'Mercado', 'PRODUTO' : 'Produto',
                            'SEGMENTO' : 'Segmento', 'ATIVO' : 'Ativo', 'VENCIMENTO' : 'Vencimento',
                            'QUANTIDADE' : 'Quantidade', 'valor_bruto' : 'Valor Bruto',
                            'valor_liq' : 'Valor Líquido'}, inplace = True)
    
    base_btg_clientes1 = 'SELECT * FROM `pristine-bonito-301012.ccmteste.base_btg`'
    base_btg_clientes = pd.read_gbq(base_btg_clientes1)
    base_btg_clientes.rename(columns = {'profissao': 'Profissao', 'Aniversário' : 'Aniversario', }
                             , inplace = True) 
    base_btg_clientes['Aniversario'] = pd.to_datetime(base_btg_clientes.Aniversario).dt.tz_localize(None) 
    base_btg_clientes['Idade'] = (dt.datetime.today() - base_btg_clientes.Aniversario) / 365
    base_btg_clientes.Idade = base_btg_clientes.Idade.astype('str')
    for i in range(base_btg_clientes.shape[0]):
        base_btg_clientes.Idade[i] = base_btg_clientes.Idade[i][:2] 

    dados = base_btg_clientes.drop(['Nome', 'Codigo_do_Escritorio', 'Escritorio', 
                                    'Codigo_do_Assessor', 'Assessor', 'Cidade',
                                    'Aniversario', 'email', 'data_abertura', 'data_vinculo',
                                   'primeiro_aporte', 'ult_aporte'],
                                    axis = 1)

    for i in range(dados.shape[0]):
        if dados.Profissao[i] != 'nan':
            dados.Profissao[i] = dados.Profissao[i].upper() 

    for i in range(dados.shape[0]):
        if dados.Profissao[i] != 'nan' and dados.Profissao[i].find('(') != -1:
            dados.Profissao[i] = dados.Profissao[i][:-4] 

    for i in range(dados.shape[0]):
        if dados.Profissao[i] != 'nan' and dados.Profissao[i].find('/') != -1:
            dados.Profissao[i] = dados.Profissao[i][:dados.Profissao[i].find('/')-1]

    for i in range(dados.shape[0]):
        if dados.Profissao[i] != 'nan' and dados.Profissao[i].find('(A)') != -1:
            dados.Profissao[i] = dados.Profissao[i][:dados.Profissao[i].find('A')-1]

    for i in range(dados.shape[0]):
        if dados.Profissao[i] == 'EMPRESÁRIO':
            dados.Profissao[i] = 'EMPRESARIO'
        elif dados.Profissao[i] == 'AGENTE AUTÔNOMO DE INVESTIMENTO':
            dados.Profissao[i] = 'AGENTE AUTONOMO DE INVESTIMENTO'
        elif dados.Profissao[i] == 'PSICÓLOGO ':
            dados.Profissao[i] = 'PSICOLOGO'
        elif dados.Profissao[i] == 'FÍSICO':
            dados.Profissao[i] = 'FISICO'
        elif dados.Profissao[i] == 'ESTAGIÁRIO':
            dados.Profissao[i] = 'ESTAGIARIO'
        elif dados.Profissao[i] == 'MÉDICO':
            dados.Profissao[i] = 'MEDICO'
        elif dados.Profissao[i] == 'FONOAUDIÓLOGO':
            dados.Profissao[i] = 'FONOAUDIOLOGO'
        elif dados.Profissao[i] == 'BANCÁRIO':
            dados.Profissao[i] = 'BANCARIO'
        elif dados.Profissao[i] == 'MATEMÁTICO':
            dados.Profissao[i] = 'MATEMATICO'
        elif dados.Profissao[i] == 'SECURITÁRIO':
            dados.Profissao[i] = 'SECURITARIO'
        elif dados.Profissao[i] == 'AGRÔNOMO':
            dados.Profissao[i] = 'AGRONOMO'
        elif dados.Profissao[i] == 'FARMACÊUTICO':
            dados.Profissao[i] = 'FARMACEUTICO'
        elif dados.Profissao[i] == 'FUNCIONÁRIO PÚBLICO':
            dados.Profissao[i] = 'FUNCIONARIO PUBLICO'
        elif dados.Profissao[i] == 'COMISSÁRIO ':
            dados.Profissao[i] = 'COMISSARIO'
        elif dados.Profissao[i] == 'ESTATÍSTICO':
            dados.Profissao[i] = 'ESTATISTICO'
        elif dados.Profissao[i] == 'COMERCIÁRIO':
            dados.Profissao[i] = 'COMERCIARIO'
        elif dados.Profissao[i] == 'AUTÔNOMO':
            dados.Profissao[i] = 'AUTONOMO' 

    dados2 = pd.get_dummies(columns = ['Tipo','Profissao','Estado_Civil', 'Estado',
        'Perfil_do_Cliente', 'Tipo_Investidor', 'Faixa_Cliente', 'Idade'], data = dados)
    dados3 = dados2.drop('Conta', axis = 1)

    carteira = []
    for i in range(base_btg_produtos.shape[0]):
        if base_btg_produtos['Mercado'][i:i+1][i] == 'Derivativos' or base_btg_produtos['Mercado'][i:i+1][i] == 'Renda Variável':
            carteira.append(base_btg_produtos['Produto'][i:i+1][i]) 
    carteira = np.unique(carteira)
    carteira = carteira.astype('O') 
    for j in range(len(carteira)):
        carteira[j] = carteira[j] + '.SA'

    yesterday = dt.date.today() - dt.timedelta(1)
    yesterday = str(yesterday) 
    dados_precos = pd.DataFrame() 
    not_find = ['ABCB2.SA', 'AERI3.SA', 'BBASN361.SA', 'BBDCC285.SA', 'BBDCC293.SA', 'BBDCN234.SA', 'BBDCN256.SA',
               'BBDCN271.SA', 'BBDCO197.SA', 'BBDCO256.SA', 'BOVAN110.SA', 'BRMLN820.SA',
               'CNTO3.SA', 'COGNB480.SA', 'COGNC40.SA', 'COGNC750.SA', 'CPTS12.SA', 'CSNAC389.SA', 
               'CSNAN32.SA', 'CSNAO329.SA', 'CVBI12.SA', 'CYREB320.SA', 'DMMO11.SA', 'GGBRB268.SA',
               'GGBRB280.SA', 'GGBRC267.SA', 'GGBRN251.SA','GGBRO221.SA','GGBRO251.SA','IRBRB105.SA',
               'IRBRB760.SA', 'IRBRB800.SA', 'IRBRC107.SA', 'IRBRC115.SA', 'IRBRN610.SA', 'ITUBC301.SA',
               'ITUBN297.SA', 'ITUBO252.SA', 'ITUBO292.SA', 'JBSSB270.SA', 'LAME1.SA', 'LAME2.SA',
               'OUJP12.SA', 'PETRB299.SA', 'PETRB317.SA', 'PETRC309.SA', 'PTAX800.SA', 'RLOG3.SA',
               'TIET11.SA', 'VALEN937.SA','VALEO922.SA', 'VALEO962.SA', 'VILG14.SA', 'VIVR1.SA', 'VVARB155.SA',
               'VVARB160.SA', 'VVARC155.SA', 'VVARC170.SA']    
    
    for ativo in carteira: 
        if ativo not in not_find:
            dados_precos[ativo] = pdr.DataReader(ativo, data_source = 'yahoo', 
            start = '2014-01-01')['Adj Close'] 
            
    dados_precos = dados_precos.fillna(method = 'bfill') 
    retorno = dados_precos.pct_change()
    retorno = retorno.iloc[1:] 
    retorno_anual = retorno.mean() * 250
    cov_diaria = retorno.cov()
    cov_anual = cov_diaria * 250

    SEED = 42 
    np.random.seed(SEED)
    kmeans = KMeans(n_clusters = 150, n_init = 10, max_iter = 300, random_state = 42) 
    kmeans.fit(dados3)    
    dados['Clusters'] = kmeans.labels_

    dados_nomes = pd.merge(base_btg_produtos.copy(), dados.copy(), 
                    on = 'Conta', how = 'left', suffixes = ('_p','_c')) 

    categoria = dados_nomes.Categoria
    segmento = dados_nomes.Segmento
    dados_nomes['Categoria-Segmento'] = categoria + '-' + segmento

    dados_usuarios = pd.read_gbq('SELECT * FROM `pristine-bonito-301012.ccmteste.login`')
    dados_usuarios = dados_usuarios.drop_duplicates()
    dados_usuarios = dados_usuarios.reset_index() 
    return dados_nomes, dados_usuarios, base_btg_produtos, dados_precos, retorno_anual, cov_anual

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_AS_ASCII'] = False
app.secret_key = 'ccm'

dados_nomes, dados_usuarios, base_btg_produtos, dados_precos, retorno_anual, cov_anual = trata_e_roda()

@app.route('/home')
def home():
    return 'Olá, mundo'

@app.route('/recomenda/<int:conta>') 
def recomenda(conta): 
    dados_filtrado = dados_nomes[['Conta','Tipo','Profissao', 'Estado_Civil', 'Estado', 'Perfil_do_Cliente',
                                 'Tipo_Investidor', 'Faixa_Cliente', 'Idade']][dados_nomes['Conta'] == conta]
    dados_filtrado2 = base_btg_produtos[['Conta', 'Mercado', 'Produto', 'Segmento', 'Ativo', 
                                         'Categoria']][base_btg_produtos['Conta'] == conta]
    localiza = dados_nomes[dados_nomes['Conta'] == conta] 
    colunas = dados_filtrado.columns.values  
    colunas2 = dados_filtrado2.columns.values 
    cluster = localiza.Clusters[0:1].values[0] 
    recomendacoes = dados_nomes[dados_nomes.Clusters == cluster] 
    recomendacoes = recomendacoes['Categoria-Segmento'][recomendacoes.Clusters == cluster].unique()
    recomendacoes = [recomendacoes[i] for i in range(len(recomendacoes))] 
    tamanho_recomendacao = len(recomendacoes) 
    
    produtos_carteira = np.unique(base_btg_produtos[['Produto']][(base_btg_produtos['Conta'] == conta) 
            & ((base_btg_produtos['Mercado'] == 'Derivativos') | (base_btg_produtos['Mercado'] == 'Renda Variável'))].values)
    produtos_carteira = produtos_carteira + '.SA'
    produtos_carteira1 = [produtos_carteira[i] for i in range(len(produtos_carteira)) if produtos_carteira[i] in
                          dados_precos.columns.values] 
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

    return jsonify(Dados=json_final)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=100)   