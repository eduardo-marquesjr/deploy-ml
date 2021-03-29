from flask import Flask, render_template, request, redirect, session, flash, url_for
from flask_basicauth import BasicAuth
import pandas as pd
import os


app = Flask(__name__)
app.config['BASIC_AUTH_USERNAME'] = os.environ['BASIC_AUTH_USERNAME']
app.config['BASIC_AUTH_PASSWORD'] = os.environ['BASIC_AUTH_PASSWORD']
app.secret_key = 'ccm'

basic_auth = BasicAuth(app)

dados_final = pd.read_excel('Dados Final Potenza.xlsx') 
dados_nomes = pd.read_excel('Dados Nomes Potenza.xlsx') 
dados_usuarios = pd.read_csv('potenza.csv')

@app.route('/home') 
# @basic_auth.required
def home():
    if 'usuario_logado' not in session or session['usuario_logado'] == None:
        return redirect(url_for('login', proxima=url_for('home')))  
    lista_contas = sorted(list(dados_final['Conta'].unique()))  
    return render_template('visual_potenza.html', contas = lista_contas)

@app.route('/recomenda/' , methods = ['POST']) 
def recomenda():
    conta = int(request.form['conta']) 
    dados_filtrado = dados_final[['Conta','Mercado','Produto','Ativo','Segmento','Categoria']][dados_final['Conta'] == conta]
    localiza = dados_final[['Conta','Mercado','Produto','Segmento','Ativo','Categoria','Clusters']][dados_final['Conta'] == conta]
    colunas = dados_filtrado.columns.values  
    lista_cluster = localiza['Clusters'].unique() 
    recomendacoes = dados_nomes[dados_nomes['Clusters'].isin(lista_cluster)] 
    # recomendacoes = recomendacoes.drop(localiza['Produto'].index, axis = 0)  
    recomendacoes = recomendacoes[['Produto','Segmento','Categoria']]
    return render_template('recomendacao.html', dados_filtrado = dados_filtrado, 
                                    colunas = colunas, recomendacoes = recomendacoes)

@app.route('/login')
def login():
    proxima = request.args.get('proxima')
    return render_template('login_potenza.html', proxima=proxima)

@app.route('/autenticar', methods=['POST', ])
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