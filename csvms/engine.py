from csvms.table import Table
from mo_sql_parsing import parse

#Usando o parse do mo sql parsing para utilizar o dict de output na criação da tabela
print(parse("CREATE TABLE lista_frutas (nm_fruta TEXT ,tp_fruta TEXT )"))


#Criação da tabela lista_frutas
Table(name="lista_frutas", 
        columns={
                'nm_fruta':str, 
                'tp_fruta': str}).save()

#Chamando a tabela lista_frutas
table = Table('lista_frutas')

#Adicionando dados à tabela
table.append('banana', 'doce')
table.append('limão', 'amargo')
table.append('bergamota', 'azedo')
table.append('maçã', 'doce')
print(table)
table.save()

#Adicionando dados de outra forma
Table(name="lista_frutas2", 
        columns={
                'nm_fruta':str, 
                'tp_fruta': str},
        data=
                [
                ('banana', 'doce'),
                ('limão', 'amargo'),
                ('bergamota', 'azedo'),
                ('maçã', 'doce')
                ]).save()