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
from tratamento import get_tabela, get_tabela_cotas
warnings.filterwarnings('ignore') 

def trata_e_roda():
    # -*- coding: utf-8 -*-
    base_btg_clientes = get_tabela('base_btg') 
    base_btg_produtos = get_tabela('posicao_potenza') 
    segmento_ativo = get_tabela("segmento_ativo") 

    base_btg_produtos.rename(columns = {'CONTA': 'Conta', 'NOME' : 'Nome', 'MERCADO': 'Mercado', 
                                    'PRODUTO' : 'Produto', 'SEGMENTO' : 'Segmento', 'ATIVO' : 'Ativo',
                                    'VENCIMENTO' : 'Vencimento',
                                    'QUANTIDADE' : 'Quantidade', 'VALOR_BRUTO_DEC_1' : 'Valor Bruto'},
                                     inplace = True)
    base_btg_clientes.rename(columns = {'profissao': 'Profissao', 'aniversario' : 'Aniversario'},
                                     inplace = True) 

    base_btg_clientes['Aniversario'] = pd.to_datetime(base_btg_clientes.Aniversario).dt.tz_localize(None) 
    base_btg_clientes['Idade'] = (dt.datetime.today() - base_btg_clientes.Aniversario) / 365
    base_btg_clientes.Idade = base_btg_clientes.Idade.astype('str')
    for i in range(base_btg_clientes.shape[0]):
        base_btg_clientes.Idade[i] = base_btg_clientes.Idade[i][:2] 

    dados = base_btg_clientes.drop(['Nome', 'cod_do_escritorio', 'escritorio', 'cod_assessor', 'Assessor',
                                'Cidade','Aniversario', 'email', 'quantidade_ativos', 'quantidade_fundos',
                                'quantidade_renda_fixa', 'quantidade_renda_variavel',
                                'quantidade_previdencia', 'quantidade_derivativos','quantidade_transito',
                                'PL_Total', 'Conta_Corrente', 'Fundos', 'Renda_Fixa', 'Renda_Variavel',
                                'Previdencia', 'Derivativos', 'Valor_em_transito',
                                'Renda_Anual', 'PL_Declarado', 'Data_de_Abertura', 'Data_Vínculo',
                                '1_aporte', 'ultimo_aporte', 'Qtd_de_Aportes', 'Aportes', 'Retiradas',
                                'Data_texto', 'Data', 'captacao_liquida', 'Estimativa_PL'],
                                 axis = 1)

    dados['Profissao'] = dados['Profissao'].astype('str') 
    
    for i in range(dados.shape[0]):
        if dados.Profissao[i] != 'nan':
            dados.Profissao[i] = dados.Profissao[i].upper() 
    
    for i in range(dados.shape[0]):
        if dados.Profissao[i] != 'nan' and dados.Profissao[i].find('(') != -1 and dados.Profissao[i] != 'ATOR, PRODUTOR OU DIRETOR (A) DE ARTES' and dados.Profissao[i] != 'COMISSÁRIO (A) DE BORDO' and dados.Profissao[i] != 'MEMBRO (A) DO PODER JUDICIÁRIO' and dados.Profissao[i] != 'PREPARADOR FÍSICO (A) / PERSONAL TRAINER' and dados.Profissao[i] != 'TREINADOR / TÉCNICO (A) ESPORTIVO' and dados.Profissao[i] != 'TÉCNICO (A) DE CONTABILIDADE E DE ESTATÍSTICA': 
            dados.Profissao[i] = dados.Profissao[i][:-4] 
    
    for i in range(dados.shape[0]):
        if dados.Profissao[i] != 'nan' and dados.Profissao[i].find('/') != -1 and dados.Profissao[i] != 'PREPARADOR FÍSICO (A) / PERSONAL TRAINER' and dados.Profissao[i] != 'TREINADOR / TÉCNICO (A) ESPORTIVO':
            dados.Profissao[i] = dados.Profissao[i][:dados.Profissao[i].find('/')-1] 
    
    for i in range(dados.shape[0]):
        if dados.Profissao[i] != 'nan' and dados.Profissao[i].find('(A)') != -1 and dados.Profissao[i] != 'ATOR, PRODUTOR OU DIRETOR (A) DE ARTES' and dados.Profissao[i] != 'COMISSÁRIO (A) DE BORDO' and dados.Profissao[i] != 'MEMBRO (A) DO PODER JUDICIÁRIO' and dados.Profissao[i] != 'PREPARADOR FÍSICO (A) / PERSONAL TRAINER' and dados.Profissao[i] != 'TREINADOR / TÉCNICO (A) ESPORTIVO' and dados.Profissao[i] != 'TÉCNICO (A) DE CONTABILIDADE E DE ESTATÍSTICA':
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
        elif dados.Profissao[i] == 'PUBLICITÁRIO':
            dados.Profissao[i] = 'PUBLICITARIO'
        elif dados.Profissao[i] == 'GEÓLOGO':
            dados.Profissao[i] = 'GEOLOGO'
    
    dados = dados.drop_duplicates() 
    dados.reset_index(drop = True, inplace = True) 
    dados2 = pd.get_dummies(columns = ['Tipo','Profissao','Estado_Civil', 'Estado',
        'Perfil_do_Cliente', 'Tipo_Investidor', 'Faixa_Cliente', 'Idade'], data = dados) 

    dados3 = dados2.drop('Conta', axis = 1) 

    SEED = 42 
    np.random.seed(SEED)
    kmeans = KMeans(n_clusters = 150, n_init = 10, max_iter = 300, random_state = 42) 
    kmeans.fit(dados3)    
    dados['Clusters'] = kmeans.labels_
 
    segmento_ativo.rename(columns = {'PRODUTO' : 'Produto', 'SEGMENTO' : 'Segmento'}, inplace = True) 
    segmento_ativo.loc[len(segmento_ativo)] = ['CONTA CORRENTE', 'Conta Corrente',
                                           'Conta Corrente', 'Conta Corrente', 'Conta Corrente'] 
    
    dados_produtos = base_btg_produtos.drop(['Nome','EMISSOR', 'Vencimento', 'Quantidade',
                                            'Valor Bruto', 'Soma_de_IR', 
                                           'Soma_de_IOF', 'ESCRITÓRIO', 'Data', 'Data_texto'], axis = 1) 
    dados_produtos = dados_produtos.drop_duplicates()
    dados_produtos.reset_index(drop = True, inplace = True)  

    dados_produtos = pd.merge(dados_produtos.copy(), segmento_ativo[['Produto','Segmento']].copy(), 
                    on = 'Produto', how = 'right', suffixes = ('_p','_c'))  

    dados_nomes = pd.merge(dados_produtos.copy(), dados.copy(), 
                    on = 'Conta', how = 'right', suffixes = ('_p','_c')) 
    
    categoria = dados_nomes.Categoria
    segmento = dados_nomes.Segmento
    dados_nomes['Categoria-Segmento'] = categoria + '-' + segmento 

    carteira = np.unique(base_btg_produtos.Produto[(base_btg_produtos.Mercado == 'Renda Variável') |
                            (base_btg_produtos.Mercado == 'Derivativos')].unique()) 
    carteira = carteira.astype('O') 
    for j in range(len(carteira)):
        carteira[j] = carteira[j] + '.SA'

    yesterday = dt.date.today() - dt.timedelta(1) 
    yesterday = str(yesterday) 
    dados_precos = pd.DataFrame() 
    not_find = ['ABCB2.SA', 'AERI3.SA', 'BBASN361.SA', 'BBDCC285.SA', 'BBDCC293.SA', 'BBDCN234.SA',
                'BBDCN256.SA', 'BBDCN271.SA', 'BBDCO197.SA', 'BBDCO256.SA', 'BOVAN110.SA', 'BRMLN820.SA',
                'MINI INDICE BOVESPA FUTURO.SA','Microcontrato de Índice S&P500.SA',
                'INDICE BOVESPA FUTURO.SA', 'CNTO3.SA', 'COGNB480.SA', 'COGNC40.SA', 'COGNC750.SA',
                'CPTS12.SA', 'CSNAC389.SA', 'CSNAN32.SA', 'CSNAO329.SA', 'CVBI12.SA', 'CYREB320.SA',
                'DMMO11.SA', 'GGBRB268.SA', 'GGBRB280.SA', 'GGBRC267.SA', 'GGBRN251.SA','GGBRO221.SA',
                'GGBRO251.SA','IRBRB105.SA', 'IRBRB760.SA', 'IRBRB800.SA', 'IRBRC107.SA', 'IRBRC115.SA',
                'IRBRN610.SA', 'ITUBC301.SA', 'ITUBN297.SA', 'ITUBO252.SA', 'ITUBO292.SA', 'JBSSB270.SA',
                'LAME1.SA', 'LAME2.SA', 'OUJP12.SA', 'PETRB299.SA', 'PETRB317.SA', 'PETRC309.SA',
                'PTAX800.SA', 'RLOG3.SA', 'TIET11.SA', 'VALEN937.SA','VALEO922.SA', 'VALEO962.SA',
                'VILG14.SA', 'VIVR1.SA', 'VVARB155.SA', 'VVARB160.SA', 'VVARC155.SA', 'VVARC170.SA']          
    for ativo in carteira: 
        if ativo not in not_find:
            dados_precos[ativo] = yf.download(ativo, start = '2014-01-01', end = yesterday)['Adj Close']

    dados_precos = dados_precos.fillna(method = 'bfill') 
    dados_precos.dropna(axis = 1, inplace = True) 

    dados_produtos = dados_produtos[dados_produtos.Conta.notna()]
    dados_produtos.reset_index(drop = True, inplace = True)

    return dados_nomes, dados_produtos, dados_precos