![IGTI logo](https://github.com/diegorech/igti-data-engineer-airflow-dags/blob/main/assets/igtilogo.jpg)

# IGTI Data Engineer Bootcamp 
## Airflow do básico ao paralelismo
Módulo 1 do Bootcamp de Engenharia de Dados da IGTI

> As DAGS encontradas na pasta `/dags` seguem uma ordem de complexidade, `treino_01.py` possui mais comentários sobre componentes básicos do Airflow e da criação de uma rotina básica.



### **treino_01.py** - Definição da DAG
Primeira DAG, mostra detalhes da criação da DAG e a utilização dos operadores Bash e Python, documenta a criação dos `defaults_args` para a DAG com mais detalhes.

![treino-1 tree view](https://github.com/diegorech/igti-data-engineer-airflow-dags/blob/main/assets/treino1-tree.jpg)

### **treino_02.py** - Encadeamento
DAG trabalha com 3 tasks para demonstrar o encadeamento
    - get_data - Um BashOperator que realiza o download dos dados do Titanic do github
    - task_idade_media - Um PythonOperator que calcula a média da idade dos passageiros e retorna esse valor para o context
    - task_print_idade - Um PythonOperator que recebe o valor da média das idade das XComs e printa em tela

![treino-2 tree view](https://github.com/diegorech/igti-data-engineer-airflow-dags/blob/main/assets/treino2-tree.jpg)

### **treino_03.py** - Condicionais

DAG trabalha com 5 tasks para demonstrar o funcionamento das condicionais no Airflow
 - get_data - Um BashOperator que realiza o download dos dados do Titanic do github
 - chooses_f_m - Um PythonOperator responsável pelo sorteio entre 'male' e 'female' para decidir de quem será feita a média da idade
 - female_male - Um PythonOperator que recebe o retorno de `chooses_f_m` e dependendo do resultado executa a task correpondente ao sexo sorteado 
 - female_branch - Um PythonOperator que calcula a média da idade dos passageiros de sexo femino
 - male_branch - Um PythonOperator que calcula a média da idade dos passageiros de sexo masculino


![treino-3 tree view](https://github.com/diegorech/igti-data-engineer-airflow-dags/blob/main/assets/treino3-tree.jpg)

### **treino_04.py** 
DAG replica um processo completo de ETL num total de 13 tasks listadas e documentadas abaixo:

- start_processing - Task inicial, confirma o inicio da rotina. BashOperator.
- task_get_data - BashOperator realiza o download dos dados do INEP através de um `wget` em formato `zip`.
- task_unzip_data - PythonOperator, realiza o unzip do arquivo na pasta raíz do download.
- task_apply_filter - PythonOperator, através do arquivo raw gera um novo csv na pasta raíz, passando um filtrando apenas as colunas necessárias.
> Schema final: 

>    `cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE', 'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']`
- *task_construct_centralized_age* - PythonOperator, calcula o valor centralizado da idade dos participantes.

- *task_construct_centralized_pow* - PythonOperator, calcula o valor centralizado ao quadrado da idade dos participantes.
- *task_construct_martial_status* - PythonOperator, através da coluna original, utilizando o dicionário de dados, transforma os valores de estado civil para seus valores originas.
- *task_construct_color* - PythonOperator, através da coluna original, utilizando o dicionário de dados, transforma os valores de cor e raça para seus valores originas e gera um arquivo csv do resultado na pasta raíz.
- *task_construct_escopai* - PythonOperator, através da coluna original, utilizando o dicionário de dados, traduz os valores de escolaridade da parte paterna e gera um arquivo csv do resultado na pasta raíz.
- *task_construct_escomae* - PythonOperator, através da coluna original, utilizando o dicionário de dados, traduz os valores de escolaridade da parte materna e gera um arquivo csv do resultado na pasta raíz.
- *task_construct_renda* - PythonOperator, através da coluna original, utilizando o dicionário de dados, traduz os valores de renda do participante e gera um arquivo csv do resultado na pasta raíz.
- *task_join* - PythonOperator, une todos os CSVs gerados na etapa de tranformação e gera um CSV contendo o novo DataFrame completo. 
- *task_load_data* - PythonOperator, insere o CSV tratado em uma tabela num banco de dados MySQL.

![treino-4 tree view](https://github.com/diegorech/igti-data-engineer-airflow-dags/blob/main/assets/treino4-tree.jpg)


## Orquestração das tasks
![orquestração das tasks](https://github.com/diegorech/igti-data-engineer-airflow-dags/blob/main/assets/orquestracao.jpg)