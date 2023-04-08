

Este repositório contém os arquivos e código fonte para executar o Desafio Big Data/BI.

Escopo do Desafio
O objetivo do desafio é fazer a ingestão dos dados de um banco relacional de vendas em arquivos .csv na pasta /raw, executar processamento e transformações de dados utilizando Spark e Hive, e então gravar as informações em tabelas dimensionais em formato csv delimitado por ';' na pasta desafio_curso/gold. Por fim, um relatório em PowerBI deve ser criado para exibir gráficos de vendas.

Etapas
Etapa 1: Enviar os arquivos para o HDFS
Etapa 2: Criar o banco DESAFIO_CURSO e dentro tabelas no Hive usando o HQL e executando um script shell dentro do hive server na pasta scripts/pre_process.
Etapa 3: Processar os dados no Spark efetuando suas devidas transformações criando os arquivos com a modelagem de BI.
Etapa 4: Gravar as informações em tabelas dimensionais em formato cvs delimitado por ';'.
Etapa 5: Exportar os dados para a pasta desafio_curso/gold
Etapa 6: Criar e editar o PowerBI com os dados que você trabalhou. No PowerBI criar gráficos de vendas.
Etapa 7: Criar uma documentação com os testes e etapas do projeto.

Regras
Campos strings vazios deverão ser preenchidos com 'Não informado'.
Campos decimais ou inteiros nulos ou vazios, deversão ser preenchidos por 0.
Atentem-se a modelagem de dados da tabela FATO e Dimensão.
Na tabela FATO, pelo menos a métrica valor de venda é um requisito obrigatório.
Nas dimensões deverá conter valores únicos, não deverá conter valores repetidos.

Estrutura do Repositório
/raw: contém os arquivos csv originais.
/scripts: contém os scripts necessários para a execução do projeto.
pre_process.sh: script shell para criar o banco de dados e as tabelas no Hive.
process.py: script Python para processar os dados no Spark e gerar as tabelas dimensionais.
/desafio_curso: contém as tabelas geradas pelo script process.py, bem como o relatório em PowerBI.
/gold: contém as tabelas dimensionais geradas em formato csv delimitado por ';'.
/app: contém o template do relatório em PowerBI.


A documentação do projeto está disponível no arquivo DOCUMENTACAO.md. Ele contém os testes tanto no PowerBi quanto no spark para confirmação dos testes.
