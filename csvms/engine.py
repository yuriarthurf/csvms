from csvms.table import Table
from mo_sql_parsing import parse

def interpretador_sql(query):

        #Usando o parse do mo sql parsing para utilizar o dict de output na criação da tabela
        traduzido = parse(query)

        #Criação da tabela lista_frutas
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

interpretador_sql("CREATE TABLE lista_frutas (nm_fruta TEXT ,tp_fruta TEXT )")