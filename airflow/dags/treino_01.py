# Primeira DAG com Airflow

from airflow import DAG
# Importação de operadores
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


# Argumentos default
default_args = {
    'owner': 'diego.rech', # Dono da DAG
    'depends_on_past': False, # Se DAG depende de algo acontecendo antes para iniciar o processo
    'start_date': datetime(2020, 11, 29, 7), # Data de inicio do processo da DAG
    'email': 'fake@hotmail.com', # Email para ser notificado, caso configurado
    'email_on_failure': False, # Para receber emails em casa de falha
    'email_on_retry': False, # Para receber emails em casa de uma nova tentativa de execução
    'retries': 1, # Quantas vezes uma nova tentativa deve ser feita
    'retry_delay': timedelta(minutes=1), # Quanto tempo até a nova tentativa ser realizada
}


# Denifinição da DAG
dag = DAG(
    'treino-01',# Nome da DAG
    description='Básico de BashOperators e PythonOperators', # Descrição que facilita a identificação da DAG
    default_args=default_args,
    schedule_interval=timedelta(minutes=2) # Intervalo de execução
)


# Adição de tarefas

hello_bash = BashOperator(
    task_id='Hello_Bash', # Identificador da task
    bash_command='echo "Hello Airflow from bash"', # Comando bash que será executado
    dag=dag
)



## PythonOperators precisa que a função que sera executada seja definida antes da definição da task

def say_hello():
    print('Hello Airflow from Python')

hello_python = PythonOperator(
    task_id='Hello_Python', # Identificador da task
    python_callable=say_hello, # Qual sera o execuável chamado pela task
    dag=dag
)


# Construção do encadeamento

hello_bash >> hello_python
