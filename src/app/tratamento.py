#encoding: utf-8
import pandas as pd
import datetime as dt
import math
import numpy as np
from sklearn.cluster import KMeans
from sklearn import metrics 
import warnings
warnings.filterwarnings('ignore')

def trata_e_roda():
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

    dados = base_btg_clientes.drop(['Nome','Aniversario','Cidade','Codigo_do_Escritorio', 'Escritorio',
                                    'Codigo_do_Assessor', 'Assessor','email',
                                    'data_abertura', 'data_vinculo', 'primeiro_aporte', 'ult_aporte'], axis = 1)

    dados['Profissao'] = dados['Profissao'].astype('str')

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
    return dados_nomes, dados_usuarios
