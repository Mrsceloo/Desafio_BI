#Importação da Sessão, dataframe e funções necessárias
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import when, col, sum, count, isnan, round, dayofmonth, month, year, desc,trim
from pyspark.sql.functions import regexp_replace, concat_ws, sha2, rtrim, substring
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
import os
import re

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

#Criação dos dataframes
df_clientes = spark.sql("select * from desafio_curso.clientes")
df_divisao = spark.sql("select * from desafio_curso.divisao")
df_endereco = spark.sql("select * from desafio_curso.endereco")
df_regiao = spark.sql("select * from desafio_curso.regiao")
df_vendas = spark.sql("select * from desafio_curso.vendas")

#Retirada de do Cabeçalho dos dataframes
df_clientes = df_clientes.subtract(df_clientes.limit(1))
df_divisao = df_divisao.subtract(df_divisao.limit(1))
df_endereco = df_endereco.subtract(df_endereco.limit(1))
df_regiao = df_regiao.subtract(df_regiao.limit(1))
df_vendas = df_vendas.subtract(df_vendas.limit(1))

#Substituição da virula pelo ponto para transformação do tipo String para Double/Int no df_vendas
df_vendas = df_vendas.withColumn("sales_amount", regexp_replace("sales_amount", ",", "."))\
.withColumn("discount_amount", regexp_replace("discount_amount", ",", "."))\
.withColumn("list_price", regexp_replace("list_price", ",", "."))\
.withColumn("sales_amount_based_on_list_price", regexp_replace("sales_amount_based_on_list_price", ",", "."))\
.withColumn("sales_cost_amount", regexp_replace("sales_cost_amount", ",", "."))\
.withColumn("sales_margin_amount", regexp_replace("sales_margin_amount", ",", "."))\
.withColumn("sales_price", regexp_replace("sales_price", ",", "."))

#Transformação para o tipo Double/Int no df_vendas
df_vendas = df_vendas.withColumn("customerkey", col("customerkey").cast("int"))\
.withColumn("discount_amount", col("discount_amount").cast("double"))\
.withColumn("invoice_number", col("invoice_number").cast("int"))\
.withColumn("item_number", col("item_number").cast("int"))\
.withColumn("line_number", col("line_number").cast("int"))\
.withColumn("list_price", col("list_price").cast("double"))\
.withColumn("order_number", col("order_number").cast("int"))\
.withColumn("sales_amount", col("sales_amount").cast("double"))\
.withColumn("sales_amount_based_on_list_price", col("sales_amount_based_on_list_price").cast("double"))\
.withColumn("sales_cost_amount", col("sales_cost_amount").cast("double"))\
.withColumn("sales_margin_amount", col("sales_margin_amount").cast("double"))\
.withColumn("sales_price", col("sales_price").cast("double"))\
.withColumn("sales_quantity", col("sales_quantity").cast("int"))\
.withColumn("sales_rep", col("sales_rep").cast("int"))

#Transformação para o tipo date no df_vendas
df_vendas = df_vendas.withColumn("actual_delivery_date", to_date("actual_delivery_date", "dd/MM/yyyy")) \
.withColumn("datekey", to_date("datekey", "dd/MM/yyyy")) \
.withColumn("invoice_date", to_date("invoice_date", "dd/MM/yyyy")) \
.withColumn("promised_delivery_date", to_date("promised_delivery_date", "dd/MM/yyyy"))

#Criação das Colunas de dia, mês e ano no df_vendas
df_vendas = df_vendas.withColumn("day", dayofmonth(col("invoice_date"))) \
.withColumn("month", month(col("invoice_date"))) \
.withColumn("year", year(col("invoice_date")))

#Tansformação para o tipo Int no df_clientes
df_clientes = df_clientes.withColumn("address_number", col("address_number").cast("int"))\
.withColumn("business_unit", col("business_unit").cast("int")).withColumn("customerkey", col("customerkey").cast("int"))\
.withColumn("division", col("division").cast("int")).withColumn("region_code", col("region_code").cast("int"))

#Transformação para o tipo Int no df_endereco
df_endereco = df_endereco.withColumn("address_number", col("address_number").cast("int"))

#Laço de repetição para detecção de campos vazios e substituição por "Não Informado" caso seja string, e 0 caso sejo tipo Number no df_vendas
colsnum = ["customerkey", "discount_amount", "invoice_number", "item_number", "line_number", "list_price", "order_number", "sales_amount", "sales_amount_based_on_list_price", "sales_cost_amount", "sales_margin_amount", "sales_price", "sales_quantity", "sales_rep"]
colsstr = ["item_class", "item", "u_m"] 
for coluna in colsnum + colsstr: # combinando as duas listas
    if coluna in colsstr:
        vendas = df_vendas.withColumn(coluna, when(trim(df_vendas[coluna]) == "", "Não informado").otherwise(df_vendas[coluna]))
    else:
        df_vendas = df_vendas.withColumn(coluna, when(trim(df_vendas[coluna]) == "", 0).otherwise(df_vendas[coluna]))

#Laço de repetição para detecção de campos vazios e substituição por "Não Informado" caso seja string, e 0 caso sejo tipo Number no df_clientes
colsnum = ["address_number", "business_unit", "customerkey", "division", "phone", "region_code"]
colsstr = ["line_of_business", "business_family", "customer", "customer_type", "regional_sales_mgr", "search_type"] 
for coluna in colsnum + colsstr: # combinando as duas listas
    if coluna in colsstr:
        df_clientes = df_clientes.withColumn(coluna, when(trim(df_clientes[coluna]) == "", "Não informado").otherwise(df_clientes[coluna]))
    else:
        df_clientes = df_clientes.withColumn(coluna, when(trim(df_clientes[coluna]) == "", 0).otherwise(df_clientes[coluna]))

#Laço de repetição para detecção de campos vazios e substituição por "Não Informado" caso seja string, e 0 caso sejo tipo Number no df_endereco
colsstr = ["city", "country", "customer_address_1", "customer_address_2", "customer_address_3", "customer_address_4", "state", "zip_code"]
colsnum = ["address_number"]
for coluna in colsnum + colsstr:
    if coluna in colsstr:
        df_endereco = df_endereco.withColumn(coluna, when(trim(df_endereco[coluna]) == "", "Não informado").otherwise(df_endereco[coluna]))
    else:
        df_endereco = df_endereco.withColumn(coluna, when(trim(df_endereco[coluna]) == "", 0).otherwise(df_endereco[coluna]))

#Junção da tabela Clientes com a tabela Divisão
df_table1 = df_clientes.join(df_divisao,df_clientes.division == df_divisao.division,"inner").drop(df_divisao.division)

#Junção da tabela 1 com Região
df_table2 = df_table1.join(df_regiao, df_table1.region_code == df_regiao.region_code, "inner").drop(df_regiao.region_code)

#Junção da tabela 2 com Endereço
df_table3 = df_table2.join(df_endereco,df_table2.address_number == df_endereco.address_number, "inner").drop(df_endereco.address_number)

#Tabela final com todos os dataframes
df_stage = df_vendas.join(df_table3, df_vendas.customerkey == df_table3.customerkey, "inner").drop(df_table3.customerkey)

#Criação do hash "pk_tempo"
df_stage = df_stage.withColumn('pk_tempo', sha2(concat_ws("",df_stage.invoice_date, df_stage.day, df_stage.month, df_stage.year ), 256))

#Criação do hash "pk_localidade"
df_stage = df_stage.withColumn('pk_localidade', sha2(concat_ws("",df_stage.address_number,df_stage.city, df_stage.country, df_stage.customer_address_1, df_stage.customer_address_2, df_stage.customer_address_3, df_stage.customer_address_4, df_stage.division, df_stage.region_code, df_stage.state), 256))

#Criação do hash "pk_clientes"
df_stage = df_stage.withColumn('pk_clientes', sha2(concat_ws("",df_stage.business_family, df_stage.business_unit, df_stage.customer, df_stage.customer_type, df_stage.line_of_business, df_stage.regional_sales_mgr, df_stage.search_type), 256))

#Criação da Dimensão tempo
dim_tempo = df_stage.select(col("pk_tempo"), col("invoice_date"), col("day"), col("month"), col("year")).dropDuplicates()

#Criação da Dimensão localidade
dim_localidade = df_stage.select(col("pk_localidade"), col("address_number"), col("city"), col("country"), col("customer_address_1"), col("customer_address_2"), col("customer_address_3"), col("customer_address_4"), col("division"), col("region_code"), col("state")).dropDuplicates()

#Criação da Dimensão clientes
dim_clientes = df_stage.select(col("pk_clientes"), col("business_family"), col("business_unit"), col("customer"), col("customer_type"), col("line_of_business"), col("regional_sales_mgr"), col("search_type")).dropDuplicates()

#Criação da Fato
ft_vendas = df_stage.select("pk_clientes", "pk_localidade", "pk_tempo", "sales_amount", "sales_quantity") \
                  .groupBy("pk_clientes", "pk_localidade", "pk_tempo") \
                  .agg(round(sum(col("sales_amount")), 2).alias("Valor de Venda"), sum(col("sales_quantity")).alias("Quantidade"))

#Troca de . para vírgula para carregamento ja no fomato ideal para o PowerBi
ft_vendas = ft_vendas.withColumn("Valor de Venda", regexp_replace("Valor de Venda", "\\.", ","))

#Função para salvar as tabelas e exportar na pasta gold
def salvar_df(df, file):
    output = "/input/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

#Salvando a dimensão tempo
salvar_df(dim_tempo,"dim_tempo")

#Salvando a dimensão localiadde
salvar_df(dim_localidade,"dim_localidade")

#Salvando a dimensão clientes
salvar_df(dim_clientes,"dim_clientes")

#Salvando fato vendas
salvar_df(ft_vendas,"ft_vendas")

