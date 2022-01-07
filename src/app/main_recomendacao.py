#encoding: utf-8
from flask import Flask, render_template, request, redirect, session, flash, url_for
import pandas as pd
import datetime as dt
import numpy as np
import os
from tratamento import trata_e_roda
from pandas_datareader import data as pdr

app = Flask(__name__)
app.secret_key = 'ccm'

dados_nomes, dados_usuarios, base_btg_produtos, dados_precos, retorno_anual, cov_anual = trata_e_roda()

print(f'Start API {dt.datetime.now()}')

@app.route('/home') 
def home():
    if 'usuario_logado' not in session or session['usuario_logado'] == None:
        return redirect(url_for('login', proxima=url_for('home')))  
    lista_contas = sorted(list(dados_nomes['Conta'].unique()))  
    return render_template('visual_potenza.html', contas = lista_contas)

@app.route('/recomenda/' , methods = ['POST']) 
def recomenda():
    conta = int(request.form['conta']) 
    dados_filtrado = dados_nomes[['Conta','Tipo','Profissao', 'Estado_Civil', 'Estado', 'Perfil_do_Cliente',
                                 'Tipo_Investidor', 'Faixa_Cliente', 'Idade']][dados_nomes['Conta'] == conta]
    dados_filtrado2 = base_btg_produtos[['Conta', 'Mercado', 'Produto', 'Segmento', 'Ativo', 
                                         'Categoria']][base_btg_produtos['Conta'] == conta]
    localiza = dados_nomes[dados_nomes['Conta'] == conta] 
    colunas = dados_filtrado.columns.values  
    colunas2 = dados_filtrado2.columns.values
    cluster = localiza.Clusters.unique()[0] 
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
    
    return render_template('recomendacao.html', dados_filtrado = dados_filtrado, colunas = colunas, 
                                    recomendacoes = recomendacoes, tamanho_recomendacao = tamanho_recomendacao,
                                    dados_filtrado2 = dados_filtrado2, colunas2 = colunas2,
                                      carteira_maior_sharpe = carteira_maior_sharpe, colunas3 = colunas3) 

@app.route('/login')
def login():
    proxima = request.args.get('proxima')
    return render_template('login_potenza.html', proxima=proxima)

@app.route('/autenticar', methods=['POST'])
def autenticar():
    usuario = request.form['usuario']
    senha = request.form['senha']
    localiza_usuario = dados_usuarios[dados_usuarios['id'] == usuario]
    if  localiza_usuario['senha'].values == senha:
        session['usuario_logado'] = usuario
        # flash(usuario.nome + ' logou com sucesso!') 
        proxima_pagina = request.form['proxima']
        return redirect(proxima_pagina)
    else:
        flash('Não logado, tente novamente!')
        return redirect(url_for('login'))

@app.route('/logout')
def logout():
    session['usuario_logado'] = None
    flash('Nenhum usuário logado!')
    return redirect(url_for('home'))

if __name__ == '__main__':
    app.run(debug=True, port=100, host='0.0.0.0')     