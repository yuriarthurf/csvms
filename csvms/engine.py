from csvms.table import Table
from mo_sql_parsing import parse
import re

def interpretador_sql(query):
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
                #print(values[k].split(',')[0], values[k].split(',')[1])
                table.append(values[k].split(',')[0].replace("'", ""), values[k].split(',')[1].replace("'", ""))

        table.save()

        return print(table) 

interpretador_sql("""INSERT INTO lista_frutas VALUES ('banana','doce');
INSERT INTO lista_frutas VALUES ('limão','amargo');
INSERT INTO lista_frutas VALUES ('bergamota','azedo');
INSERT INTO lista_frutas VALUES ('maçã','doce');""")