CREATE TABLE IF NOT EXISTS desafio_curso.REGIAO(
region_code string, region_name string)
COMMENT 'Tabela de Regiao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/REGIAO/'
TBLPROPERTIES("skip.header.line.count"="1");
