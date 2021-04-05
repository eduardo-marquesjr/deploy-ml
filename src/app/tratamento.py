import pandas as pd
import datetime as dt
import math
import numpy as np
from sklearn.cluster import KMeans
from sklearn import metrics 
import warnings
warnings.filterwarnings('ignore')

def trata_e_roda():
    base_btg_produtos = pd.read_excel('../../data/raw/Ultima_Data_-_POSIÇÃO.xlsx')
    base_btg_produtos.rename(columns = {'CONTA': 'Conta', 'MERCADO': 'Mercado', 'PRODUTO' : 'Produto',
                            'SEGMENTO' : 'Segmento', 'ATIVO' : 'Ativo', 'VENCIMENTO' : 'Vencimento',
                            'QUANTIDADE' : 'Quantidade', 'VALOR BRUTO' : 'Valor Bruto',
                            'VALOR LÍQUIDO' : 'Valor Líquido'}, inplace = True)

    base_btg_clientes = pd.read_excel('../../data/raw/base_btg.xls')
    base_btg_clientes['Aniversário'] = pd.to_datetime(base_btg_clientes.Aniversário).dt.tz_localize(None) 
    base_btg_clientes['Idade'] = (dt.datetime.today() - base_btg_clientes.Aniversário) / 365
    base_btg_clientes.Idade = base_btg_clientes.Idade.astype('str')
    for i in range(base_btg_clientes.shape[0]):
        base_btg_clientes.Idade[i] = base_btg_clientes.Idade[i][:2] 

    dados = base_btg_clientes.drop(['Nome','Aniversário','Cidade','Código do Escritório', 'Escritório',
                                    'Código do Assessor', 'Assessor','E-mail', 'Qtd de Ativos',
                                    'Qtd Fundos', 'Qtd Renda Fixa', 'Qtd Renda Variável',
                                    'Qtd Previdência', 'Qtd Derivativos', 'Qtd Valor em Trânsito',
                                    'PL Total', 'Conta Corrente', 'Fundos', 'Renda Fixa',
                                    'Renda Variável', 'Previdência', 'Derivativos',
                                    'Valor em Trânsito', 'Renda Anual', 'PL Declarado',
                                    'Data de Abertura', 'Data Vínculo', '1º Aporte', 'Último Aporte',
                                    'Qtd de Aportes', 'Aportes', 'Retiradas', 'Data (texto)', 'Data'],
                                    axis = 1)

    dados['Profissão'] = dados['Profissão'].astype('str')

    for i in range(dados.shape[0]):
        if dados.Profissão[i] != 'nan':
            dados.Profissão[i] = dados.Profissão[i].upper() 

    for i in range(dados.shape[0]):
        if dados.Profissão[i] != 'nan' and dados.Profissão[i].find('(') != -1:
            dados.Profissão[i] = dados.Profissão[i][:-4] 

    for i in range(dados.shape[0]):
        if dados.Profissão[i] != 'nan' and dados.Profissão[i].find('/') != -1:
            dados.Profissão[i] = dados.Profissão[i][:dados.Profissão[i].find('/')-1]

    for i in range(dados.shape[0]):
        if dados.Profissão[i] != 'nan' and dados.Profissão[i].find('(A)') != -1:
            dados.Profissão[i] = dados.Profissão[i][:dados.Profissão[i].find('A')-1]

    for i in range(dados.shape[0]):
        if dados.Profissão[i] == 'EMPRESÁRIO':
            dados.Profissão[i] = 'EMPRESARIO'
        elif dados.Profissão[i] == 'AGENTE AUTÔNOMO DE INVESTIMENTO':
            dados.Profissão[i] = 'AGENTE AUTONOMO DE INVESTIMENTO'
        elif dados.Profissão[i] == 'PSICÓLOGO ':
            dados.Profissão[i] = 'PSICOLOGO'
        elif dados.Profissão[i] == 'FÍSICO':
            dados.Profissão[i] = 'FISICO'
        elif dados.Profissão[i] == 'ESTAGIÁRIO':
            dados.Profissão[i] = 'ESTAGIARIO'
        elif dados.Profissão[i] == 'MÉDICO':
            dados.Profissão[i] = 'MEDICO'
        elif dados.Profissão[i] == 'FONOAUDIÓLOGO':
            dados.Profissão[i] = 'FONOAUDIOLOGO'
        elif dados.Profissão[i] == 'BANCÁRIO':
            dados.Profissão[i] = 'BANCARIO'
        elif dados.Profissão[i] == 'MATEMÁTICO':
            dados.Profissão[i] = 'MATEMATICO'
        elif dados.Profissão[i] == 'SECURITÁRIO':
            dados.Profissão[i] = 'SECURITARIO'
        elif dados.Profissão[i] == 'AGRÔNOMO':
            dados.Profissão[i] = 'AGRONOMO'
        elif dados.Profissão[i] == 'FARMACÊUTICO':
            dados.Profissão[i] = 'FARMACEUTICO'
        elif dados.Profissão[i] == 'FUNCIONÁRIO PÚBLICO':
            dados.Profissão[i] = 'FUNCIONARIO PUBLICO'
        elif dados.Profissão[i] == 'COMISSÁRIO ':
            dados.Profissão[i] = 'COMISSARIO'
        elif dados.Profissão[i] == 'ESTATÍSTICO':
            dados.Profissão[i] = 'ESTATISTICO'
        elif dados.Profissão[i] == 'COMERCIÁRIO':
            dados.Profissão[i] = 'COMERCIARIO'
        elif dados.Profissão[i] == 'AUTÔNOMO':
            dados.Profissão[i] = 'AUTONOMO' 

    dados2 = pd.get_dummies(columns = ['Tipo','Profissão','Estado Civil', 'Estado',
        'Perfil do Cliente', 'Tipo Investidor', 'Faixa Cliente', 'Idade'], data = dados)
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

    dados_usuarios = pd.read_csv('../../data/processed/potenza.csv', sep = ';')
    return dados_nomes, dados_usuarios
