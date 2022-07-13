from csvms.table import Table
from mo_sql_parsing import parse
import re

def interpretador_sql(query):
        global table

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

        if 'UPDATE' in query:
                parsed_query = parse(query)

                if 'SET' or 'set' in query:
                        for indice in range(len(table)):
                                if table[indice][list(parsed_query['set'])[0]] == 'amargo':
                                        linha = table[indice]
                                        linha[list(parsed_query['set'])[0]] = list(parsed_query['set']['tp_fruta'].values())[0]
                                        table[indice] = tuple(linha.values())
                                        table.save()
                return print(table)

        if 'DELETE' in query:
                query_traduzida = parse(query)
                for i in range(len(table)):
                        if table[i][query_traduzida['where']['eq'][0]] == (list(query_traduzida['where']['eq'][1].values()))[0]:
                                del table[i]
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

interpretador_sql("""UPDATE lista_frutas
   SET tp_fruta = 'azedo'
   WHERE nm_fruta = 'limão'""")

interpretador_sql("""DELETE FROM lista_frutas 
 WHERE nm_fruta = 'limão'""")