#encoding: utf-8
# trata e roda...
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
import time
warnings.filterwarnings('ignore')

def get_tabela(nome_tabela):
    import mysql.connector 
    mydb = mysql.connector.connect( 
    host = "10.0.0.2",
    port = "3306", 
    user = 'dataprep_potenza_ro',
    passwd = 'ZfGo#kXMi1MNw52LAvOalt3-HYEatN',
    database = "dataprep_potenza",
    auth_plugin = 'mysql_native_password',
    use_unicode=True, 
    charset="utf8") 
    mycursor = mydb.cursor() 
    select = "SELECT * FROM" + ' ' + nome_tabela + ' ' + "LIMIT 50000" 
    mycursor.execute(select) 
    myresult = mycursor.fetchall() 
    tabela_banco = pd.DataFrame(myresult, columns = mycursor.column_names) 
    for coluna in tabela_banco.columns:
        for i in range(len(tabela_banco[coluna])):
            if type(tabela_banco[coluna][i]) == bytearray or type(tabela_banco[coluna][i]) == bytes:
                tabela_banco[coluna][i] = str(tabela_banco[coluna][i], 'utf-8') 
    
    return tabela_banco

def get_tabela_cotas(ano_mes):
    base_btg_produtos = get_tabela('posicao_potenza') 
    fundos_cadastro_btg = get_tabela("fundos_cad_btg")
    base_btg_produtos.rename(columns = {'CONTA': 'Conta', 'NOME' : 'Nome', 'MERCADO': 'Mercado', 
                                    'PRODUTO' : 'Produto', 'SEGMENTO' : 'Segmento', 'ATIVO' : 'Ativo',
                                    'VENCIMENTO' : 'Vencimento',
                                    'QUANTIDADE' : 'Quantidade', 'VALOR_BRUTO_DEC_1' : 'Valor Bruto'},
                                     inplace = True) 
    dados_produtos = base_btg_produtos.drop(['Nome','EMISSOR', 'Vencimento', 'Quantidade',
                                            'Valor Bruto', 'Soma_de_IR', 
                                           'Soma_de_IOF', 'ESCRITÓRIO', 'Data', 'Data_texto'], axis = 1) 
    dados_produtos = dados_produtos.drop_duplicates() 
    dados_produtos.reset_index(drop = True, inplace = True)  
    fundos_cadastro_btg.rename(columns = {'PRODUTO' : 'Produto'}, inplace = True)
    base_btg_fundos = dados_produtos[['Produto']][dados_produtos.Mercado == 'Fundos'] 

    base_btg_fundos_cnpj = pd.merge(base_btg_fundos, fundos_cadastro_btg, 
                                on = 'Produto', how = 'left', suffixes = ('_l', '_r')) 
    
    fundos_btg_cnpj = base_btg_fundos_cnpj[base_btg_fundos_cnpj.CNPJ.notna()]
    fundos_btg_cnpj = fundos_btg_cnpj.sort_values('Produto')
    fundos_btg_cnpj.reset_index(drop = True, inplace = True)

    url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_' + str(ano_mes) + '.csv'
    tabela = pd.read_csv(url, sep = ';')     
    tabela = tabela[['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA']] 
    tabela.rename(columns = {'CNPJ_FUNDO' : 'CNPJ', 'DT_COMPTC' : 'Date',
                   'VL_QUOTA' : 'Valor_Cota'}, inplace = True) 
    tabela['CNPJ'] = tabela['CNPJ'].apply(lambda x : str(x).replace('.',''))   
    tabela['CNPJ'] = tabela['CNPJ'].apply(lambda x : str(x).replace('/',''))
    tabela['CNPJ'] = tabela['CNPJ'].apply(lambda x : str(x).replace('-',''))
    dados_fundos = pd.merge(tabela.copy(), fundos_btg_cnpj.copy(),
                            on = 'CNPJ', how = 'inner', suffixes = ('_l', '_r')) 
    dados_fundos.drop('CNPJ', axis = 1, inplace = True) 
    dados_fundos = dados_fundos.groupby('Produto').agg(list)
    dados_fundos = dados_fundos.reset_index() 
    dados_fundos['tamanho'] = [len(dados_fundos.Date[n]) for n in range(dados_fundos.shape[0])]
    colunas = dados_fundos.Produto.unique()
    datas = dados_fundos.Date[dados_fundos[dados_fundos.tamanho == dados_fundos.tamanho.max()].index[0]] 
    df = pd.DataFrame(columns = colunas, index = datas)   
    for i in range(dados_fundos.shape[0]):
        if dados_fundos.Produto[i] == df.columns.values[i]:
            if df[df.columns.values[i]].shape[0] == len(dados_fundos.Valor_Cota[i]):
                df[df.columns.values[i]] = dados_fundos.Valor_Cota[i] 
            else:
                for k in range(df[df.columns.values[i]].shape[0] - len(dados_fundos.Valor_Cota[i])):
                    dados_fundos.Valor_Cota[i].append(np.mean(dados_fundos.Valor_Cota[i])) 
                df[df.columns.values[i]] = dados_fundos.Valor_Cota[i]   
    return df 