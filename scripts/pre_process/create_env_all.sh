#!/bin/bash

DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

for i in "${DADOS[@]}"
do
	#Criando as pastas 
	echo "Criando a pasta $i"
	cd /input/raw/
	hdfs dfs -mkdir /datalake/raw/$i
	
	#Movendo os arquivos da pasta input para dentro da pasta raw do HDFS
	echo "Movendo os arquivos para dentro do HDFS"
	hdfs dfs -chmod 777 /datalake/raw/$i
	hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i

	#Criar as tabelas dentro do hive dentro da database desafio_curso
	beeline -u jdbc:hive2://localhost:10000 -f /input/scripts/hql/create_table_$i.hql 
done

