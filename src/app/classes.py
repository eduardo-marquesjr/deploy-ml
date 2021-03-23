class Negociacao:
    def __init__(self, ciclo_Vendas, 
        proprietario_Ana_Benati, proprietario_Durval_Almeida,
        proprietario_Felipe_Moraes, proprietario_Fernanda_Rosa, 
        proprietario_Guilherme_Salgarelle, proprietario_Joao_Holtz, 
        proprietario_Matheus_Ramos, proprietario_Renato_Lazzarini, proprietario_Roberto_Kroll, 
        tipo_De_Servico_Avulso, tipo_De_Servico_Recorrente,
        meses_Contrato_12, meses_Contrato_24, meses_Contrato_36, meses_Contrato_48,
        meses_Contrato_60, meses_Contrato_Avulso, 
        modelo_Contratacao, modelo_Renegociacao,
        expectativa_Alta, expectativa_Boa, expectativa_Desvantagem, expectativa_Media,
        expectativa_Praticamente_Perdida, expectativa_Sondagem, 
        produtos_Backup, produtos_DBA, produtos_Email, produtos_Firewall, 
        produtos_Gestao_De_AD, produtos_Horas_Tecnicas, produtos_Licenças, 
        produtos_Nuvem, produtos_TS, produtos_Upgrade,
        segmento_Agropecuaria, segmento_Outros, segmento_Servicos_Especializados,
        segmento_TI_Telecom, segmento_Veiculos_e_Pecas, 
        fonte_Parceiro_Buscador, fonte_Parceiro_Cold_Call, 
        fonte_Parceiro_Contato_Site, fonte_Parceiro_Distribuidor, 
        fonte_Parceiro_Email, fonte_Parceiro_Evento, 
        fonte_Parceiro_Indicacao, fonte_Parceiro_LinkedIn, 
        fonte_Parceiro_Lista, fonte_Parceiro_Parceiros,
        n_Funcionarios_101_a_200, n_Funcionarios_2_a_5, n_Funcionarios_201_a_500,
        n_Funcionarios_26_a_50, n_Funcionarios_501_a_1000, n_Funcionarios_51_a_100,
        n_Funcionarios_6_a_25, n_Funcionarios_Individual, n_Funcionarios_Mais_de_1000,
        indicacao_Contato_cadastrado_na_base, indicacao_Linx, indicacao_Não_houve_indicacao,
        indicacao_Sankhya, indicacao_Tecinco, 
        tipo_de_negocio_Existente, tipo_de_negocio_Novo_cliente,
        equipe_de_TI_Apenas_um, equipe_de_TI_Equipe_Interna, equipe_de_TI_Interna_Terceirizada,
        equipe_de_TI_Nao_possui, equipe_de_TI_Terceirizada, 
        regiao_Centro_Oeste, regiao_Nordeste, regiao_Norte, regiao_Sudeste, regiao_Sul, 
        banco_de_Dados_Dois_ou_mais, banco_de_Dados_Firebird, banco_de_Dados_Informix, 
        banco_de_Dados_MySQL, banco_de_Dados_Oracle, banco_de_Dados_Outro, 
        banco_de_Dados_PostgreSQL, banco_de_Dados_SQL_Server, 
        eRP_Apollo, eRP_BRX, eRP_Dealernet, eRP_Dois_ou_mais, eRP_Globus, 
        eRP_Linx, eRP_NBS, eRP_Oracle, eRP_Outro, eRP_Protheus, eRP_SAP, 
        eRP_Sances, eRP_Sankhya, eRP_Senior, eRP_Sercon, eRP_Sisdia, 
        eRP_Solucao_Propria, eRP_Spress, eRP_Tecinco, eRP_Totvs, 
        categoria_Receita_A, categoria_Receita_B, categoria_Receita_C,
        categoria_Receita_D, categoria_Receita_E, categoria_Receita_F, 
        categoria_Receita_G, 
        categoria_Desconto_A, categoria_Desconto_B, categoria_Desconto_C, 
        categoria_Desconto_D, categoria_Desconto_E, categoria_Desconto_F, 
        categoria_Desconto_G, categoria_Margem_A, categoria_Margem_B, 
        categoria_Margem_C, categoria_Margem_D, categoria_Margem_E, 
        categoria_Margem_F): 
        self.ciclo_Vendas = ciclo_Vendas
        self.proprietario_Ana_Benati = proprietario_Ana_Benati 
        self.proprietario_Durval_Almeida = proprietario_Durval_Almeida
        self.proprietario_Felipe_Moraes = proprietario_Felipe_Moraes
        self.proprietario_Fernanda_Rosa = proprietario_Fernanda_Rosa
        self.proprietario_Guilherme_Salgarelle = proprietario_Guilherme_Salgarelle
        self.proprietario_Joao_Holtz = proprietario_Joao_Holtz
        self.proprietario_Matheus_Ramos = proprietario_Matheus_Ramos
        self.proprietario_Renato_Lazzarini = proprietario_Renato_Lazzarini
        self.proprietario_Roberto_Kroll = proprietario_Roberto_Kroll
        self.tipo_De_Servico_Avulso = tipo_De_Servico_Avulso
        self.tipo_De_Servico_Recorrente = tipo_De_Servico_Recorrente
        self.meses_Contrato_12 = meses_Contrato_12
        self.meses_Contrato_24 = meses_Contrato_24
        self.meses_Contrato_36 = meses_Contrato_36
        self.meses_Contrato_48 = meses_Contrato_48
        self.meses_Contrato_60 = meses_Contrato_60
        self.meses_Contrato_Avulso = meses_Contrato_Avulso
        self.modelo_Contratacao = modelo_Contratacao
        self.modelo_Renegociacao = modelo_Renegociacao
        self.expectativa_Alta = expectativa_Alta
        self.expectativa_Boa = expectativa_Boa
        self.expectativa_Desvantagem  = expectativa_Desvantagem
        self.expectativa_Media = expectativa_Media
        self.expectativa_Praticamente_Perdida  = expectativa_Praticamente_Perdida
        self.expectativa_Sondagem = expectativa_Sondagem
        self.produtos_Backup = produtos_Backup
        self.produtos_DBA = produtos_DBA
        self.produtos_Email = produtos_Email
        self.produtos_Firewall = produtos_Firewall
        self.produtos_Gestao_De_AD = produtos_Gestao_De_AD
        self.produtos_Horas_Tecnicas = produtos_Horas_Tecnicas
        self.produtos_Licenças = produtos_Licenças
        self.produtos_Nuvem = produtos_Nuvem
        self.produtos_TS = produtos_TS
        self.produtos_Upgrade = produtos_Upgrade
        self.segmento_Agropecuaria = segmento_Agropecuaria
        self.segmento_Outros = segmento_Outros
        self.segmento_Servicos_Especializados = segmento_Servicos_Especializados
        self.segmento_TI_Telecom = segmento_TI_Telecom
        self.segmento_Veiculos_e_Pecas = segmento_Veiculos_e_Pecas
        self.fonte_Parceiro_Buscador = fonte_Parceiro_Buscador
        self.fonte_Parceiro_Cold_Call = fonte_Parceiro_Cold_Call
        self.fonte_Parceiro_Contato_Site = fonte_Parceiro_Contato_Site
        self.fonte_Parceiro_Distribuidor = fonte_Parceiro_Distribuidor
        self.fonte_Parceiro_Email = fonte_Parceiro_Email
        self.fonte_Parceiro_Evento = fonte_Parceiro_Evento
        self.fonte_Parceiro_Indicacao = fonte_Parceiro_Indicacao 
        self.fonte_Parceiro_LinkedIn = fonte_Parceiro_LinkedIn
        self.fonte_Parceiro_Lista = fonte_Parceiro_Lista
        self.fonte_Parceiro_Parceiros = fonte_Parceiro_Parceiros
        self.n_Funcionarios_101_a_200 = n_Funcionarios_101_a_200
        self.n_Funcionarios_2_a_5 = n_Funcionarios_2_a_5
        self.n_Funcionarios_201_a_500 = n_Funcionarios_201_a_500
        self.n_Funcionarios_26_a_50 = n_Funcionarios_26_a_50
        self.n_Funcionarios_501_a_1000 = n_Funcionarios_501_a_1000
        self.n_Funcionarios_51_a_100 = n_Funcionarios_51_a_100
        self.n_Funcionarios_6_a_25 = n_Funcionarios_6_a_25
        self.n_Funcionarios_Individual = n_Funcionarios_Individual
        self.n_Funcionarios_Mais_de_1000 = n_Funcionarios_Mais_de_1000
        self.indicacao_Contato_cadastrado_na_base = indicacao_Contato_cadastrado_na_base
        self.indicacao_Linx = indicacao_Linx
        self.indicacao_Não_houve_indicacao = indicacao_Não_houve_indicacao
        self.indicacao_Sankhya = indicacao_Sankhya
        self.indicacao_Tecinco = indicacao_Tecinco
        self.tipo_de_negocio_Existente = tipo_de_negocio_Existente
        self.tipo_de_negocio_Novo_cliente = tipo_de_negocio_Novo_cliente
        self.equipe_de_TI_Apenas_um = equipe_de_TI_Apenas_um
        self.equipe_de_TI_Equipe_Interna = equipe_de_TI_Equipe_Interna
        self.equipe_de_TI_Interna_Terceirizada = equipe_de_TI_Interna_Terceirizada
        self.equipe_de_TI_Nao_possui = equipe_de_TI_Nao_possui
        self.equipe_de_TI_Terceirizada = equipe_de_TI_Terceirizada
        self.regiao_Centro_Oeste = regiao_Centro_Oeste
        self.regiao_Nordeste = regiao_Nordeste
        self.regiao_Norte = regiao_Norte
        self.regiao_Sudeste = regiao_Sudeste
        self.regiao_Sul = regiao_Sul
        self.banco_de_Dados_Dois_ou_mais = banco_de_Dados_Dois_ou_mais
        self.banco_de_Dados_Firebird = banco_de_Dados_Firebird
        self.banco_de_Dados_Informix = banco_de_Dados_Informix
        self.banco_de_Dados_MySQL = banco_de_Dados_MySQL
        self.banco_de_Dados_Oracle = banco_de_Dados_Oracle
        self.banco_de_Dados_Outro = banco_de_Dados_Outro
        self.banco_de_Dados_PostgreSQL = banco_de_Dados_PostgreSQL
        self.banco_de_Dados_SQL_Server = banco_de_Dados_SQL_Server
        self.eRP_Apollo = eRP_Apollo
        self.eRP_BRX = eRP_BRX
        self.eRP_Dealernet = eRP_Dealernet
        self.eRP_Dois_ou_mais = eRP_Dois_ou_mais
        self.eRP_Globus = eRP_Globus
        self.eRP_Linx = eRP_Linx
        self.eRP_NBS = eRP_NBS
        self.eRP_Oracle = eRP_Oracle
        self.eRP_Outro = eRP_Outro
        self.eRP_Protheus = eRP_Protheus
        self.eRP_SAP = eRP_SAP
        self.eRP_Sances = eRP_Sances
        self.eRP_Sankhya = eRP_Sankhya
        self.eRP_Senior = eRP_Senior
        self.eRP_Sercon = eRP_Sercon
        self.eRP_Sisdia = eRP_Sisdia
        self.eRP_Solucao_Propria = eRP_Solucao_Propria
        self.eRP_Spress = eRP_Spress
        self.eRP_Tecinco = eRP_Tecinco
        self.eRP_Totvs = eRP_Totvs
        self.categoria_Receita_A = categoria_Receita_A
        self.categoria_Receita_B = categoria_Receita_B
        self.categoria_Receita_C = categoria_Receita_C
        self.categoria_Receita_D = categoria_Receita_D
        self.categoria_Receita_E = categoria_Receita_E
        self.categoria_Receita_F = categoria_Receita_F
        self.categoria_Receita_G = categoria_Receita_G
        self.categoria_Desconto_A = categoria_Desconto_A
        self.categoria_Desconto_B = categoria_Desconto_B
        self.categoria_Desconto_C = categoria_Desconto_C
        self.categoria_Desconto_D = categoria_Desconto_D
        self.categoria_Desconto_E = categoria_Desconto_E
        self.categoria_Desconto_F = categoria_Desconto_F
        self.categoria_Desconto_G = categoria_Desconto_G
        self.categoria_Margem_A = categoria_Margem_A
        self.categoria_Margem_B = categoria_Margem_B
        self.categoria_Margem_C = categoria_Margem_C
        self.categoria_Margem_D = categoria_Margem_D
        self.categoria_Margem_E = categoria_Margem_E
        self.categoria_Margem_F = categoria_Margem_F
