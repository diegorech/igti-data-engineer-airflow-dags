# igti-data-engineer-airflow-dags

Módulo 1 do Bootcamp de Engenharia de Dados da IGTI

A pasta DAGs possui os seguintes arquivos trabalhados em aula:

- treino_01.py - Primeira DAG mostrando detalhes da criação da DAG e a utilização dos operadores Bash e Python
- treino_02.py - DAG trabalha com 3 tasks para demonstrar o encadeamento
    - get_data - Um BashOperator que realiza o download dos dados do Titanic do github
    - task_idade_media - Um PythonOperator que calcula a média da idade dos passageiros e retorna esse valor para o context
    - task_print_idade - Um PythonOperator que recebe o valor da média das idade das XComs e printa em tela
- treino_03.py - DAG trabalha com 5 tasks para demonstrar o funcionamento das condicionais no Airflow
    - get_data - Um BashOperator que realiza o download dos dados do Titanic do github
    - chooses_f_m - Um PythonOperator responsável pelo sorteio entre 'male' e 'female' para decidir de quem será feita a média da idade
    - female_male - Um PythonOperator que recebe o retorno de `chooses_f_m` e dependendo do resultado executa a task correpondente ao sexo sorteado 
    - female_branch - Um PythonOperator que calcula a média da idade dos passageiros de sexo femino
    - male_branch - Um PythonOperator que calcula a média da idade dos passageiros de sexo masculino
- treino_04.py - 