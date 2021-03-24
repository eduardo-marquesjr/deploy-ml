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

dados_final = pd.read_excel('Dados Final Potenza.xlsx') 
dados_nomes = pd.read_excel('Dados Nomes Potenza.xlsx') 
print(dados_final['Conta'][0])
print(dados_nomes['Produto'][0])

@app.route('/') 
@basic_auth.required
def index():
    lista_contas = sorted(list(dados_final['Conta'].unique()))  
    return render_template('visual_potenza.html', contas = lista_contas)

@app.route('/recomenda/' , methods = ['POST']) 
def recomenda():
    conta = int(request.form['conta']) 
    dados_filtrado = dados_final[['Conta','Mercado','Produto','Ativo','Segmento','Categoria']][dados_final['Conta'] == conta]
    localiza = dados_final[['Conta','Mercado','Produto','Segmento','Ativo','Categoria','Clusters']][dados_final['Conta'] == conta]
    colunas = dados_final.columns.values 
    lista_cluster = localiza['Clusters'].unique() 
    recomendacoes = dados_nomes[dados_nomes['Clusters'].isin(lista_cluster)] 
    # recomendacoes = recomendacoes.drop(localiza['Produto'].index, axis = 0)  
    recomendacoes = recomendacoes[['Produto','Segmento','Categoria']]
    return render_template('recomendacao.html', dados_filtrado = dados_filtrado, 
                                    colunas = colunas, recomendacoes = recomendacoes)

app.run(debug = True, host='0.0.0.0') 
