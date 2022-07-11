from csvms.table import Table
from mo_sql_parsing import parse

#Usando o parse do mo sql parsing para utilizar o dict de output na criação da tabela
print(parse("CREATE TABLE lista_frutas (nm_fruta TEXT ,tp_fruta TEXT )"))


#Criação da tabela lista_frutas
table = Table(name="lista_frutas", 
                columns={
                        'nm_fruta':str, 
                        'tp_fruta': str}).save()
print(table)