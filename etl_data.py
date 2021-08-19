'''
|---------------------------------------|
| ETL de dados de ocorrências CENIPA    |
|---------------------------------------|
'''
import pandas as pd
import pandera as pa
from etl_charge_dataframe import charge_dataframe as charge
import matplotlib.pyplot as plt

# Realiza a carga total dos dados da origem no dataframe
df = charge()

'''
|---------------------------------------|
| Validação dos dados                   |
|---------------------------------------|
'''
schema = pa.DataFrameSchema(
    columns={
        'codigo_ocorrencia': pa.Column(pa.Int),
        'codigo_ocorrencia2': pa.Column(pa.Int),
        'ocorrencia_classificacao': pa.Column(pa.String),
        'ocorrencia_cidade': pa.Column(pa.String),
        'ocorrencia_uf': pa.Column(pa.String, pa.Check.str_length(2, 2), nullable=True),
        'ocorrencia_aerodromo': pa.Column(pa.String, nullable=True),
        'ocorrencia_dia': pa.Column(pa.DateTime),
        'ocorrencia_hora': pa.Column(pa.String, pa.Check.str_matches(r'^([0-1]?[0-9]|[2][0-3]):([0-5][0-9])(:[0-5][0-9])?$'), nullable=True),
        'total_recomendacoes': pa.Column(pa.Int)
    }
)
schema.validate(df)

'''
|---------------------------------------|
| Transformação e Load dos dados        |
|---------------------------------------|
'''

#cicades que obtiveram recomendacoes
#realiza a carga total dos dados da origem no dataframe
df = charge()

filtro = df.total_recomendacoes > 0
df = df.loc[filtro, ['ocorrencia_cidade', 'total_recomendacoes']]
df.to_csv('out_recomendacoes_cidade.csv', sep=',', na_rep='Unkown', header=True, index=False)


#ocorrências cuja classificação seja INCIDENTE GRAVE ou INCIDENTE, no estado de SP
#realiza a carga total dos dados da origem no dataframe
df = charge()

filtro1 = df.ocorrencia_classificacao.isin(['INCIDENTE GRAVE', 'INCIDENTE'])
filtro2 = df.ocorrencia_uf == 'SP'
df = df.loc[filtro1 & filtro2, ['ocorrencia_cidade', 'ocorrencia_aerodromo', 'ocorrencia_dia', 'ocorrencia_hora', 'ocorrencia_classificacao']]
df.to_csv('out_incidentes_graves_sp.csv', sep=',', na_rep='Unkown', header=True, index=False)

#ocorrências do ano de 2015
#realiza a carga total dos dados da origem no dataframe
df = charge()
filtro = df.ocorrencia_dia.dt.year == 2015
df = df.loc[filtro]
df.to_csv('out_2015.csv', sep=',', na_rep='Unkown', header=True, index=False)

'''
|---------------------------------------|
| Criação de gráficos estatísticos      |
|---------------------------------------|
'''
#realiza a carga total dos dados da origem no dataframe
df = charge()

# define estilo e tamanho dos gráficos
plt.style.use("ggplot")
plt.figure(figsize=(20, 12))

#quantidade de ocorrências por ano
df['ocorrencia_ano'] = df.ocorrencia_dia.dt.year
qtd_ocorr_ano = df.groupby('ocorrencia_ano')['codigo_ocorrencia'].nunique()
qtd_ocorr_ano.sort_values(ascending=False)

qtd_ocorr_ano.plot.bar(color='gray')
plt.title('quantidade de OCORRÊNCIAS nos últimos 10 anos')
plt.xlabel('anos')
plt.ylabel('ocorrências')
plt.savefig('qtd_ocorr_ano.png')

#quantidade de ACIDENTES por ano
filtro1 = df.ocorrencia_classificacao.isin(['ACIDENTE'])
qtd_acidentes_ano = df.loc[filtro1]
qtd_acidentes_ano = qtd_acidentes_ano.groupby('ocorrencia_ano')['codigo_ocorrencia'].nunique()

qtd_acidentes_ano.plot.bar(color='red')
plt.title('quantidade de ACIDENTES nos últimos 10 anos')
plt.xlabel('anos')
plt.ylabel('acidentes')
plt.savefig('qtd_acid_ano.png')

#quantidade de ocorrências por Estado
qtd_ocorr_uf = df.groupby('ocorrencia_uf')['codigo_ocorrencia'].nunique().sort_values(ascending=False)

qtd_ocorr_uf.plot.bar(color='green')
plt.title('quantidade de ACIDENTES por Estado')
plt.xlabel('UF')
plt.ylabel('acidentes')
plt.savefig('qtd_ocorr_uf.png')

if __name__ == '__main__':
    pass