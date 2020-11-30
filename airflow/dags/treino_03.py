# DAG schedulada para utilização dos dados do Titanic
# Primeira DAG com Airflow

from airflow import DAG
# Importação de operadores
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd 
import random 

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
    'treino-03',# Nome da DAG
    description='Utiliza os dados do Titanic e calcula idade média para homens ou mulheres', # Descrição que facilita a identificação da DAG
    default_args=default_args,
    schedule_interval='*/2 * * * *'# Intervalo de execução utilizando cron
    # schedule_interval=@once # Intervalo de execução
    # schedule_interval=@daily # Intervalo de execução
    # schedule_interval=@hourly # Intervalo de execução
)


# Adição de tarefas

# Get Titanic data from github
get_data = BashOperator(
    task_id='get_data',
    bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/download/train.csv',
    dag=dag
)


# Returns female or male randomly
def rand_f_m():
    return random.choice(['male', 'female'])

chooses_f_m = PythonOperator(
    task_id='chooses-f-m',
    python_callable=rand_f_m,
    dag=dag
)


def FemaleOrMale(**kwargs):
    value = kwargs['task_instance'].xcom_pull(task_ids='chooses-f-m')
    if value == 'female':
        return 'female_branch' # Retorna qual será a próxima TASK
    if value == 'male':
        return 'male_branch' # Retorna qual será a próxima TASK

female_male = BranchPythonOperator(
    task_id = 'condicional',
    python_callable = FemaleOrMale,
    provide_context = True,
    dag=dag
)


def mean_female():
    df = pd.read_csv('~/download/train.csv')
    df = df.loc[
        df.Sex == 'female'
    ]
    print(f'A média de idade das mulheres no Titanic era: {df.Age.mean()}')

female_branch = PythonOperator(
    task_id='female_branch',
    python_callable=mean_female,
    dag=dag
)

def mean_male():
    df = pd.read_csv('~/download/train.csv')
    df = df.loc[
        df.Sex == 'male'
    ]
    print(f'A média de idade dos homens no Titanic era: {df.Age.mean()}')



male_branch = PythonOperator(
    task_id='male_branch',
    python_callable=mean_male,
    dag=dag
)


# Construção do encadeamento

# Tasks entre [] porque pode ser tanto uma quanto a outra
get_data >> chooses_f_m >> female_male >> [female_branch, male_branch]
