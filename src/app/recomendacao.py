from flask import Flask, render_template, request, redirect, session, flash, url_for
from flask_basicauth import BasicAuth
import pandas as pd
import pickle
import os

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['BASIC_AUTH_USERNAME'] = os.environ.get('BASIC_AUTH_USERNAME')
app.config['BASIC_AUTH_PASSWORD'] = os.environ.get('BASIC_AUTH_PASSWORD')

basic_auth = BasicAuth(app)

@app.route('/') 
@basic_auth.required
def index():
    dados = pd.read_excel('../../data/processed/Dados Final Potenza.xlsx')
    dados = sorted(list(dados['Conta'].unique()))  
    return render_template('templates/visual_potenza.html', contas = dados)

@app.route('/recomenda/' , methods = ['POST']) 
def recomenda():
    dados = pd.read_excel('../../data/processed/Dados Final Potenza.xlsx')
    dados_nomes = pd.read_excel('../../data/processed/Dados Nomes Potenza.xlsx') 
    conta = int(request.form['conta']) 
    dados_final = dados[['Conta','Mercado','Produto','Ativo','Segmento','Categoria']][dados['Conta'] == conta]
    localiza = dados[['Conta','Mercado','Produto','Segmento','Ativo','Categoria','Clusters']][dados['Conta'] == conta]
    colunas = dados_final.columns.values 
    lista_cluster = localiza['Clusters'].unique() 
    recomendacoes = dados_nomes[dados_nomes['Clusters'].isin(lista_cluster)] 
    # recomendacoes = recomendacoes.drop(localiza['Produto'].index, axis = 0)  
    recomendacoes = recomendacoes[['Produto','Segmento','Categoria']]
    return render_template('templates/recomendacao.html', dados_filtrado = dados_final, 
                                    colunas = colunas, recomendacoes = recomendacoes)

app.run(debug = True) 
