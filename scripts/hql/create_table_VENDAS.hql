CREATE TABLE IF NOT EXISTS desafio_curso.VENDAS(
actual_delivery_date string, customerkey string, datekey string, discount_amount string, invoice_date string, invoice_number string, item_class string, item_number string, item string, line_number string , list_price string, order_number string, promised_delivery_date string, sales_amount string, sales_amount_based_on_list_price string, sales_cost_amount string, sales_margin_amount string, sales_price string, sales_quantity string, sales_rep string, u_m string)
COMMENT 'Tabela de Vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/VENDAS/'
TBLPROPERTIES("skip.header.line.count"="1");
