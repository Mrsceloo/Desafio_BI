CREATE TABLE IF NOT EXISTS desafio_curso.CLIENTES (
address_number string, business_family string, business_unit string, customer string, 
customerkey string, customer_type string, division string, line_of_business string, phone string,
 region_code string, regional_sales_mgr string, search_type string)
COMMENT 'Tabela de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/CLIENTES/'
TBLPROPERTIES("skip.header.line.count"="1");
