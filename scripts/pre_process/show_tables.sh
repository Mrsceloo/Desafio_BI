DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

for i in "${DADOS[@]}"
do
	#Verificacao das tabelas criadas
	beeline -u jdbc:hive2://localhost:10000 -e "select * from desafio_curso.$i limit 3;" 
done
