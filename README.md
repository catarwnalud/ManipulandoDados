#
# ManipulandoDados

Trabalho Prático - Labolatório de Banco de Dados

O Ministério da Saúde (MS), por meio da Secretaria de Vigilância em Saúde (SVS), desenvolve a vigilância da Síndrome Respiratória Aguda Grave (SRAG) no Brasil, desde a pandemia de Influenza A(H1N1)pdm09 em 2009. A partir disso, esta vigilância foi implantada na rede de Influenza e outros vírus respiratórios, que anteriormente atuava apenas com a vigilância sentinela de Síndrome Gripal (SG). 

Recentemente (2020), a vigilância da COVID-19, a infecção humana causada pelo novo Coronavírus, foi incorporada na rede de vigilância da Influenza e outros vírus respiratórios. 

Este projeto tem como finalidade disponibilizar o legado dos bancos de dados (BD) epidemiológicos de SRAG, da rede de vigilância da Influenza e outros vírus respiratórios, desde o início da sua implantação (2009) até os dias atuais (2020), com a incorporação da vigilância da COVID-19. Atualmente, o sistema oficial para o registro dos casos e óbitos de SRAG é o Sistema de Informação da Vigilância Epidemiológica da Gripe (SIVEP-Gripe). 

No Guia de Vigilância Epidemiológica Emergência de Saúde Pública de Importância Nacional pela Doença pelo Coronavírus 2019 estão disponíveis informações sobre definições de casos, critérios de confirmação e encerramento dos casos, dentre outros. 
As bases de dados de SRAG disponibilizadas neste portal passam por tratamento que envolve a anonimização, em cumprimento a legislação. 

Para mais informações, acessar: 
https://opendatasus.saude.gov.br/dataset/srag-2020

## Os Arquivos de Dados

Os dados de todos os arquivos somam aproximadamente 700MB. Foi preciso fazer uma engenharia reversa a partir deste dados, propondo uma estrutura no banco de dados PostgreSQL para armazená-los.

Depois de analisar os dados que estes arquivos contém, criando a estrutura do banco de dados, como importar os dados de 1.199.105 pessoas neste banco de dados? Não é viável fazer isso manualmente, por isso utilizamos a linguaguem de programção python para fazer esse trabalho.

Portanto, os principais desafios deste trabalho foram: 

1) Como usar python para se conectar ao banco de dados PostgreSQL e inserir os dados?
2) Como ler e acessar cada um dos atributos contidos nos arquivos CSV?
   
O intuito do trabalho foi aprender a manipular um arquivo CSV com a linguagem escolhida, bem como saber como se conectar e manipular o banco de dados PostgreSQL com a mesma linguagem. A tarefa deste trabalho é muito comum na vida de cientistas de dados e desenvolvedores que manipulam dados de sistemas legados, onde transformam-se dados não estruturados em banco de dados relacionais. 

Foi utulizado o banco de dados PostgreSQL 9.1.

Temos um dump do banco de dados contendo DADOS e ESTRUTURA (arquivo texto .sql) chamado README.TXT e um código da aplicação/script/programa construído para importar os dados.


