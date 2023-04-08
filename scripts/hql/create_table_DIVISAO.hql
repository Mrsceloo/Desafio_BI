CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.DIVISAO ( 
division string, division_name string)
COMMENT 'Tabela Divisao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/DIVISAO/'
TBLPROPERTIES ("skip.header.line.count"="1");
