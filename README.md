# igti-data-engineer-airflow-dags

treino_01.py - Primeira DAG mostrando detalhes da criação da DAG e a utilização dos operadores Bash e Python
treino_02.py - DAG trabalha com 3 tasks para demonstrar o encadeamento
                get_data - Um BashOperator que realiza o download dos dados do Titanic do github
                task_idade_media - Um PythonOperator que calcula a média da idade dos passageiros e retorna esse valor para o context
                task_print_idade - Um PythonOperator que recebe o valor da média das idade das XComs e printa em tela