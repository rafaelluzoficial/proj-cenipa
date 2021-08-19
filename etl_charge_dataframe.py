import pandas as pd

'''
|---------------------------------------|
| Carga e Limpeza dos dados             |
|---------------------------------------|
'''
def charge_dataframe():
    valores_ausentes = ['**', '****', '*****', '###!', '####', 'NULL']
    df = pd.read_csv('ocorrencia_2010_2020.csv', sep='\t', parse_dates=['ocorrencia_dia'], dayfirst=True, na_values=valores_ausentes)
    return df