'''
|---------------------------------------|
| ETL de dados de ocorrências CENIPA    |
|---------------------------------------|
'''
import pandas as pd
import pandera as pa
from etl_charge_dataframe import charge_dataframe as charge

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
| Transformação dos dados               |
|---------------------------------------|
'''

#cicades que obtiveram recomendacoes
#realiza a carga total dos dados da origem no dataframe
df = charge()

filtro = df.total_recomendacoes > 0
df = df.loc[filtro, ['ocorrencia_cidade', 'total_recomendacoes']]
df.to_csv('out_recomendacoes_cidade.csv', sep=',', na_rep='Unkown', float_format='%.2f', header=True, index=False)


#ocorrências cuja classificação seja INCIDENTE GRAVE ou INCIDENTE, no estado de SP
#realiza a carga total dos dados da origem no dataframe
df = charge()

filtro1 = df.ocorrencia_classificacao.isin(['INCIDENTE GRAVE', 'INCIDENTE'])
filtro2 = df.ocorrencia_uf == 'SP'
df = df.loc[filtro1 & filtro2, ['ocorrencia_cidade', 'ocorrencia_aerodromo', 'ocorrencia_dia', 'ocorrencia_hora', 'ocorrencia_classificacao']]
df.to_csv('out_incidentes_graves_sp.csv', sep=',', na_rep='Unkown', float_format='%.2f', header=True, index=False)

#ocorrências do ano de 2015
#realiza a carga total dos dados da origem no dataframe
df = charge()
filtro = df.ocorrencia_dia.dt.year == 2015
df = df.loc[filtro]
df.to_csv('out_2015.csv', sep=',', na_rep='Unkown', float_format='%.2f', header=True, index=False)

if __name__ == '__main__':
    pass