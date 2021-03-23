from classes import Negociacao
from flask import Flask, render_template, request, redirect, session, flash, url_for, jsonify
from flask_basicauth import BasicAuth
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
import pickle
import os

modelo_forecast_vendas_api = pickle.load(open('../../models/modelo_forecast_vendas_api2.pkl', 'rb'))

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['BASIC_AUTH_USERNAME'] = os.environ.get('BASIC_AUTH_USERNAME')
app.config['BASIC_AUTH_PASSWORD'] = os.environ.get('BASIC_AUTH_PASSWORD')

basic_auth = BasicAuth(app)

negociacao1 = Negociacao(632,   0,   0,   0,   0,   0,   1,   0,   0,   0,   1,  0,   0,   0,
0,   0,   0,   1,   1,   0,   0,   0,  0,   1,   0,   0,   0,   0,   0,   0,
0,   0,   0,  1,   0,   0,   0,   0,   0,   0,   1,   0,   0,   0,  0,   1,  
0,   0,   0,   0,   0,   1,   0,   0,   0, 0,   0,   0,   0,   0,   0,   0,   
0,   0,   0,   0,  1,   0,   1,   0 ,  0,   0,   0,   0,   0,   1,   0,  0,   0,   0,   0,   1, 
0,  0,   0,   1,   0,   0,  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  0,   0,
0,   0,   0,  0,   0,   0,   0,   1,   0,  0,   0,   0,   0,   1,   0,   0,   0,   0,   1,   
0,  0,   0,   0,   0)
lista = [negociacao1]

@app.route('/')
def index():
    return render_template('visual.html', titulo = 'Negociações')

@app.route('/forecast/', methods = ['POST'])
@basic_auth.required
def forecast():
    dados = request.get_json()
    if dados:
        if isinstance(dados, dict): #unique value
            df_raw = pd.DataFrame(dados, index = [0]) 
        else:
            df_raw = pd.DataFrame(dados, columns = dados[0].keys())
    previs = modelo_forecast_vendas_api.predict(df_raw)
    df_raw['prediction'] = previs
    return df_raw.to_json(orient = 'records')

app.run(debug = True)

