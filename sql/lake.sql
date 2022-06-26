--aws s3 sync bootcamp/examples/data/log s3://compass.uol.bootcamp/sql
;
CREATE EXTERNAL TABLE csvms.raw_lista_frutas(
    op string, 
    op_ts timestamp,
    nm_fruta string,
    tp_fruta string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://compass.uol.bootcamp/sql/default.lista_frutas/';

CREATE EXTERNAL TABLE csvms.raw_tipo_frutas(
    op string, 
    op_ts timestamp,
    tp_fruta string,
    vl_fruta float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://compass.uol.bootcamp/sql/default.tipo_frutas/';

CREATE EXTERNAL TABLE csvms.raw_venda_frutas(
    op string, 
    op_ts timestamp,
    cod_venda integer,
    nm_fruta string,
    qtd_venda integer)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://compass.uol.bootcamp/sql/default.venda_frutas/';

CREATE OR REPLACE VIEW lista_frutas
    AS
  WITH current as (
SELECT max(op_ts) last
     , nm_fruta
  FROM raw_lista_frutas
 GROUP BY nm_fruta)
SELECT raw_lista_frutas.nm_fruta
     , raw_lista_frutas.tp_fruta
  FROM raw_lista_frutas
  JOIN current 
    ON current.last = raw_lista_frutas.op_ts 
   AND current.nm_fruta = raw_lista_frutas.nm_fruta
 WHERE raw_lista_frutas.op <> 'D';

CREATE OR REPLACE VIEW tipo_frutas
    AS
  WITH current as (
SELECT max(op_ts) last
     , tp_fruta
  FROM raw_tipo_frutas
 GROUP BY tp_fruta)
SELECT raw_tipo_frutas.tp_fruta
     , raw_tipo_frutas.vl_fruta
  FROM raw_tipo_frutas
  JOIN current 
    ON current.last = raw_tipo_frutas.op_ts 
   AND current.tp_fruta = raw_tipo_frutas.tp_fruta
 WHERE raw_tipo_frutas.op <> 'D';

CREATE OR REPLACE VIEW venda_frutas
    AS
  WITH current as (
SELECT max(op_ts) last
     , cod_venda
  FROM raw_venda_frutas
 GROUP BY cod_venda)
SELECT raw_venda_frutas.cod_venda
     , raw_venda_frutas.nm_fruta
     , raw_venda_frutas.qtd_venda
  FROM raw_venda_frutas
  JOIN current 
    ON current.last = raw_venda_frutas.op_ts 
   AND current.cod_venda = raw_venda_frutas.cod_venda
 WHERE raw_venda_frutas.op <> 'D';

-- https://docs.aws.amazon.com/pt_br/athena/latest/ug/presto-functions.html
SELECT t.tp_fruta tipo
     , sum(if(v.qtd_venda IS NULL,0,v.qtd_venda) * t.vl_fruta) total
  FROM tipo_frutas t
  LEFT OUTER JOIN lista_frutas l ON t.tp_fruta = l.tp_fruta
  LEFT OUTER JOIN venda_frutas v ON l.nm_fruta = l.nm_fruta
 GROUP BY t.tp_fruta
 ORDER BY 2 desc
;

-- EXPLAIN
EXPLAIN ANALYZE VERBOSE  SELECT nm_fruta FROM raw_lista_frutas WHERE tp_fruta = 'doce';
-- https://prestodb.io/docs/current/optimizer.html
-- https://github.com/prestodb/presto/tree/master/presto-spi/src/main/java/com/facebook/presto/spi/plan