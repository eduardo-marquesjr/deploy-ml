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
import sys
from mysql.connector.constants import ClientFlag
warnings.filterwarnings('ignore') 

mydb = mysql.connector.connect(
    host = "10.0.0.2",
    port = "3306", 
    user = 'dataprep_potenza_ro',
    passwd = 'ZfGo#kXMi1MNw52LAvOalt3-HYEatN',
    database = "dataprep_potenza",
    auth_plugin = 'mysql_native_password',
    use_unicode=True,
    charset="utf8",
) 

mycursor = mydb.cursor() 

def get_tabela(nome_tabela):
    select = "SELECT * FROM" + ' ' + nome_tabela + ' ' + "LIMIT 500000" 
    mycursor.execute(select) 
    myresult = mycursor.fetchall() 
    tabela = pd.DataFrame(myresult, columns = mycursor.column_names) 
    for coluna in tabela.columns:
        for i in range(len(tabela[coluna])):
            if type(tabela[coluna][i]) == bytearray or type(tabela[coluna][i]) == bytes:
                tabela[coluna][i] = str(tabela[coluna][i], 'utf-8') 
                
    return tabela 

print('Carregando imagem 04/08 14:35')
print('Conectando e puxando as tabelas...')
base_btg_clientes = get_tabela('base_btg') 
base_btg_produtos = get_tabela('posicao_potenza') 

base_btg_produtos.rename(columns = {'CONTA': 'Conta', 'NOME' : 'Nome', 'MERCADO': 'Mercado', 
                                    'PRODUTO' : 'Produto', 'SEGMENTO' : 'Segmento', 'ATIVO' : 'Ativo',
                                    'VENCIMENTO' : 'Vencimento',
                                    'QUANTIDADE' : 'Quantidade', 'VALOR_BRUTO_DEC_1' : 'Valor_Bruto'},
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

from sklearn.cluster import KMeans
SEED = 42 
np.random.seed(SEED)
kmeans = KMeans(n_clusters = 150, n_init = 10, max_iter = 300, random_state = 42) 
kmeans.fit(dados3)    
dados['Clusters'] = kmeans.labels_

segmento_ativo = get_tabela("segmento_ativo") 
segmento_ativo.rename(columns = {'PRODUTO' : 'Produto', 'SEGMENTO' : 'Segmento'}, inplace = True) 
segmento_ativo.loc[len(segmento_ativo)] = ['CONTA CORRENTE', 'Conta Corrente',
                                           'Conta Corrente', 'Conta Corrente', 'Conta Corrente'] 

dados_produtos = base_btg_produtos.drop(['Nome','EMISSOR', 'Vencimento', 'Quantidade','Soma_de_IR', 
                                           'Soma_de_IOF', 'ESCRITÓRIO', 'Data_texto'], axis = 1) 
# dados_produtos = dados_produtos.drop_duplicates() 
# dados_produtos.reset_index(drop = True, inplace = True)  

dados_produtos = pd.merge(dados_produtos.copy(), segmento_ativo[['Produto','Segmento']].copy(), 
                    on = 'Produto', how = 'left', suffixes = ('_p','_c'))   

dados_nomes = pd.merge(dados_produtos.copy(), dados.copy(), 
                    on = 'Conta', how = 'right', suffixes = ('_p','_c')) 
                
categoria = dados_nomes.Categoria
segmento = dados_nomes.Segmento
dados_nomes['Categoria-Segmento'] = categoria + '-' + segmento 
dados_nomes.dropna(inplace = True) 
dados_nomes['Conta'] = dados_nomes['Conta'].astype(int) 

fundos_cadastro_btg = get_tabela("fundos_cad_btg")
fundos_cadastro_btg.rename(columns = {'PRODUTO' : 'Produto'}, inplace = True) 

base_btg_fundos = base_btg_produtos[['Produto']][base_btg_produtos.Mercado == 'Fundos'] 
base_btg_fundos = base_btg_fundos.drop_duplicates()
base_btg_fundos.reset_index(drop = True, inplace = True)

base_btg_fundos_cnpj = pd.merge(base_btg_fundos, fundos_cadastro_btg, 
                                on = 'Produto', how = 'left', suffixes = ('_l', '_r')) 

fundos_btg_cnpj = base_btg_fundos_cnpj[base_btg_fundos_cnpj.CNPJ.notna()]
fundos_btg_cnpj = fundos_btg_cnpj.sort_values('Produto')
fundos_btg_cnpj.reset_index(drop = True, inplace = True)

print('Tratando os dados....')
dados_precos_fundos = get_tabela("dados_precos_fundos")
dados_precos_fundos.set_index('Date', inplace = True) 

retorno = dados_precos_fundos.pct_change()
retorno = retorno.iloc[1:] 
retorno_anual = retorno.mean() * 250
cov_diaria = retorno.cov() 
cov_anual = cov_diaria * 250 

dados_produtos = dados_produtos[dados_produtos.Conta.notna()]
# dados_produtos = dados_produtos.drop_duplicates() 
# dados_produtos.reset_index(drop = True, inplace = True) 
dados_produtos.Segmento.fillna(0, inplace = True)   
for i in range(dados_produtos.shape[0]):
    if dados_produtos.Segmento[i] == 0:
        dados_produtos.Segmento[i] = dados_produtos.Categoria[i] 

dados_produtos['Conta'] = dados_produtos['Conta'].astype(int) 
dados_produtos['Valor_Bruto'] = dados_produtos['Valor_Bruto'].apply(lambda x : str(x).replace('.',''))   
dados_produtos['Valor_Bruto'] = dados_produtos['Valor_Bruto'].astype(float)  
dados_produtos['Data'] = pd.to_datetime(dados_produtos['Data'], format = '%d %b %Y %H:%M:%S')   
dados_produtos = dados_produtos.loc[dados_produtos.Data == dados_produtos.Data.max()]   
dados_produtos = dados_produtos.drop_duplicates() 
dados_produtos.reset_index(drop = True, inplace = True) 

dados_produtos = dados_produtos[(dados_produtos.Mercado == 'Renda Variável') | 
                               (dados_produtos.Mercado == 'Fundos') | 
                               (dados_produtos.Mercado == 'Conta Corrente') | 
                               (dados_produtos.Mercado == 'Derivativos')] 
dados_produtos = dados_produtos.drop_duplicates() 
dados_produtos.reset_index(drop = True, inplace = True) 

dados_produtos['Total'] = 0
contas = dados_produtos.Conta.unique()
for conta in contas:
    dados_produtos['Total'][dados_produtos.Conta == conta] = dados_produtos.Valor_Bruto[(dados_produtos.Conta == conta) 
                                                                 & (dados_produtos.Mercado != 'Conta Corrente')].sum() 

for i in range(dados_produtos.shape[0]):
    if dados_produtos['Mercado'][i] == 'Conta Corrente':
        dados_produtos['Total'][i] = 1

dados_produtos['Porcentagem (%)'] = dados_produtos['Valor_Bruto'] / dados_produtos['Total'] 
dados_produtos['Porcentagem (%)'] = round((dados_produtos['Porcentagem (%)'] * 100), 2) 

dados_contas = []
dados_contas_final = {} 
dados_contas_str = dados_nomes.Conta
dados_contas_str = dados_contas_str.astype('O')
for k in range(len(dados_nomes.Conta.unique())):
    dados_contas.append(dados_contas_str.unique()[k])
dados_contas_final['Contas'] = dados_contas

print('Start da API....')
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_AS_ASCII'] = False
app.secret_key = 'ccm'

@app.route('/home')
def home():
    return 'Olá, CCMers'

@app.route('/contas')
def contas():
    return jsonify(Data=dados_contas_final) 

@app.route('/recomenda/<int:conta>') 
def recomenda(conta): 
    dados_filtrado = dados_nomes[['Conta','Tipo','Profissao', 'Estado_Civil', 'Estado', 'Perfil_do_Cliente',
                                 'Tipo_Investidor', 'Faixa_Cliente', 'Idade']][dados_nomes['Conta'] == conta]
    dados_filtrado2 = dados_produtos[['Conta', 'Mercado', 'Produto', 'Segmento', 'Ativo', 
                                         'Categoria', 'Valor_Bruto', 'Porcentagem (%)']][dados_produtos['Conta'] == conta]
    localiza = dados_nomes[dados_nomes['Conta'] == conta] 
    colunas = dados_filtrado.columns.values  
    colunas2 = dados_filtrado2.columns.values 
    cluster = localiza.Clusters[0:1].values[0] 
    recomendacoes = dados_nomes[dados_nomes.Clusters == cluster] 
    recomendacoes = recomendacoes['Categoria-Segmento'].unique()
    recomendacoes = recomendacoes.astype('O') 
    recomendacoes_final = [recomendacoes[i] for i in range(len(recomendacoes)) if recomendacoes[i] != 'Conta Corrente-Conta Corrente' and recomendacoes[i] != None and recomendacoes[i] != 'NaN' and recomendacoes[i] != 'nan'] 
    tamanho_recomendacao = len(recomendacoes_final) 
    
    produtos_carteira_acoes = np.unique(dados_produtos[['Produto']][(dados_produtos['Conta'] == conta) 
            & ((dados_produtos['Mercado'] == 'Derivativos') | 
               (dados_produtos['Mercado'] == 'Renda Variável'))].values)  
    # produtos_carteira_acoes = produtos_carteira_acoes + '.SA'

    produtos_carteira_fundos = np.unique(dados_produtos[['Produto']][(dados_produtos['Conta'] == conta) 
            & (dados_produtos['Mercado'] == 'Fundos')].values)  
    for n in range(len(produtos_carteira_fundos)): 
        produtos_carteira_fundos[n] = produtos_carteira_fundos[n].replace(' ', '_')
        produtos_carteira_fundos[n] = produtos_carteira_fundos[n].replace('-', '_')
        produtos_carteira_fundos[n] = produtos_carteira_fundos[n].replace('&', 'e') 
        produtos_carteira_fundos[n] = produtos_carteira_fundos[n].replace('.', '_')

    produtos_carteira = np.append(produtos_carteira_acoes, produtos_carteira_fundos)  
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

    portfolio_ativos = {}
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
    lista_valor = [dados_filtrado2[m:m+1].values[0][6] for m in range(dados_filtrado2.shape[0])]
    lista_porcentagem = [dados_filtrado2[m:m+1].values[0][7] for m in range(dados_filtrado2.shape[0])]
    json_carteira[dados_filtrado2.columns.values[0]] = lista_conta
    json_carteira[dados_filtrado2.columns.values[1]] = lista_mercado
    json_carteira[dados_filtrado2.columns.values[2]] = lista_produto
    json_carteira[dados_filtrado2.columns.values[3]] = lista_segmento
    json_carteira[dados_filtrado2.columns.values[4]] = lista_ativo
    json_carteira[dados_filtrado2.columns.values[5]] = lista_categoria
    json_carteira[dados_filtrado2.columns.values[6]] = lista_valor
    json_carteira[dados_filtrado2.columns.values[7]] = lista_porcentagem

    json_balanco = {}
    json_portfolio = {}
    if len(carteira_maior_sharpe) > 0:
        for n in range(len(carteira_maior_sharpe.columns.values)):
            if carteira_maior_sharpe.columns.values[n] != 'Retornos' and carteira_maior_sharpe.columns.values[n] != 'Volatilidade' and carteira_maior_sharpe.columns.values[n] != 'Sharpe':
                json_balanco[carteira_maior_sharpe.columns.values[n]] = carteira_maior_sharpe[0:1].values[0][n] 
            else:
                json_portfolio[carteira_maior_sharpe.columns.values[n]] = carteira_maior_sharpe[0:1].values[0][n] 
    else:
        json_balanco = {}

    json_recomendacoes = {'Recomendacoes' : recomendacoes_final}

    json_porcentagens = {} 
    for k in range((dados_filtrado2.shape[0]) - 1):
        json_porcentagens[dados_filtrado2.Produto.values[k]] = dados_filtrado2['Porcentagem (%)'].values[k]
    # json_porcentagens.pop('CONTA CORRENTE') 

    json_final = {
        'Balanceamento' : json_balanco,
        'Carteira' : json_carteira,
        'Cliente' : json_cliente,
        'Porcentagens' : json_porcentagens,
        'Portfolio' : json_portfolio,
        'Recomendacoes' : json_recomendacoes
    }

    return jsonify(Data=json_final)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=100) 