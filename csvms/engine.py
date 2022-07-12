from csvms.table import Table
from mo_sql_parsing import parse
import re

def interpretador_sql(query):
        if 'CREATE' in query:
                traduzido = parse(query)
                try:
                        Table(name=traduzido['create table']['name'], 
                                columns={
                                        traduzido['create table']['columns'][0]['name']:str, 
                                        traduzido['create table']['columns'][1]['name']:str}).save()
                        print('Tabela criada com sucesso')
                except:
                        return print('Não foi possível criar a tabela')

                table = Table('lista_frutas')
                return print(table) 
        
        if 'INSERT' in query:
                query = query.split(";")
                new_query = []
                values = []
                for i in query:
                        new_query.append(i.replace('\n',''))

                for j in new_query:
                        values.append(new_query[new_query.index(j)].replace("INSERT INTO lista_frutas VALUES (", ''))
                
                values.pop(-1)

                table = Table('lista_frutas')

                for k in range(len(values)):
                        values[k] = values[k].replace(')', '')
                        table.append(values[k].split(',')[0].replace("'", ""), values[k].split(',')[1].replace("'", ""))

                table.save()

                return print(table) 

interpretador_sql("""CREATE TABLE lista_frutas (
    nm_fruta TEXT ,
    tp_fruta TEXT 
)""")

interpretador_sql("""INSERT INTO lista_frutas VALUES ('banana','doce');
INSERT INTO lista_frutas VALUES ('limão','amargo');
INSERT INTO lista_frutas VALUES ('bergamota','azedo');
INSERT INTO lista_frutas VALUES ('maçã','doce');""")