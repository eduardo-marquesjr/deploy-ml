#encoding: utf-8
from flask import Flask, render_template, request, redirect, session, flash, url_for
import pandas as pd
import os
from tratamento import trata_e_roda

app = Flask(__name__)
app.secret_key = 'ccm'

dados_nomes, dados_usuarios = trata_e_roda()

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
    localiza = dados_nomes[dados_nomes['Conta'] == conta]
    colunas = dados_filtrado.columns.values  
    cluster = localiza.Clusters.unique()[0] 
    recomendacoes = dados_nomes[dados_nomes.Clusters == cluster] 
    recomendacoes = recomendacoes['Categoria-Segmento'][recomendacoes.Clusters == cluster].unique()
    recomendacoes = [recomendacoes[i] for i in range(len(recomendacoes))] 
    tamanho_recomendacao = len(recomendacoes) 
    return render_template('recomendacao.html', dados_filtrado = dados_filtrado, colunas = colunas, 
                                    recomendacoes = recomendacoes, tamanho_recomendacao = tamanho_recomendacao)

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