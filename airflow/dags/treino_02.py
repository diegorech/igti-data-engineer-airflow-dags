# DAG schedulada para utilização dos dados do Titanic
# Primeira DAG com Airflow

from airflow import DAG
# Importação de operadores
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd 

# Argumentos default
default_args = {
    'owner': 'diego.rech', # Dono da DAG
    'depends_on_past': False, # Se DAG depende de algo acontecendo antes para iniciar o processo
    'start_date': datetime(2020, 11, 29, 7), # Data de inicio do processo da DAG
    'email': 'diego_airflow@hotmail.com', # Email para ser notificado, caso configurado
    'email_on_failure': False, # Para receber emails em casa de falha
    'email_on_retry': False, # Para receber emails em casa de uma nova tentativa de execução
    'retries': 1, # Quantas vezes uma nova tentativa deve ser feita
    'retry_delay': timedelta(minutes=1) # Quanto tempo até a nova tentativa ser realizada
}


# Denifinição da DAG
dag = DAG(
    'treino-02',# Nome da DAG
    description='Extrai dados do Titanic da internet e calcula idade média', # Descrição que facilita a identificação da DAG
    default_args=default_args,
    schedule_interval='*/2 * * * *'# Intervalo de execução utilizando cron
    # schedule_interval=@once # Intervalo de execução
    # schedule_interval=@daily # Intervalo de execução
    # schedule_interval=@hourly # Intervalo de execução
)


# Adição de tarefas
get_data = BashOperator(
    task_id='get_data',
    bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/download/train.csv',
    dag=dag
)

def calculate_mean_age():
    df = pd.read_csv('~/download/train.csv')
    med = df.Age.mean()
    return med # Retornar uma variável é um dos jeitos de criar uma XCom, variável compartilhável entre tasks

def print_age(**context): # Passa context para a função possuir um contexto e conseguir usar a variável retornada pela função anterior
    value = context['task_instance'].xcom_pull(task_ids='calcula-idade-media') # xcom_pull() pega o valor passado pela função anterior 
    print(f'A idade média no Titanic era {value} anos.')

task_idade_media = PythonOperator(
    task_id='calcula-idade-media',
    python_callable=calculate_mean_age,
    dag=dag
)

task_print_idade = PythonOperator(
    task_id='mostra-idade',
    python_callable=print_age,
    provide_context=True, # Config necessária para Operador que recebe xcom do contexto
    dag=dag
)


# Construção do encadeamento

get_data >> task_idade_media >> task_print_idade

