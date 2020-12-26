# DAG schedulada para utilização dos dados do Titanic


from airflow import DAG
# Importação de operadores
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd 
import zipfile 
import pyodbc
import sqlalchemy

data_path = '/root/download'

# Argumentos default
default_args = {
    'owner': 'diego.rech', # Dono da DAG
    'depends_on_past': False, # Se DAG depende de algo acontecendo antes para iniciar o processo
    'start_date': datetime(2020, 11, 30, 23), # Data de inicio do processo da DAG
    'email': 'fake@hotmail.com', # Email para ser notificado, caso configurado
    'email_on_failure': False, # Para receber emails em casa de falha
    'email_on_retry': False, # Para receber emails em casa de uma nova tentativa de execução
    'retries': 1, # Quantas vezes uma nova tentativa deve ser feita
    'retry_delay': timedelta(minutes=1) # Quanto tempo até a nova tentativa ser realizada
}


# Denifinição da DAG
dag = DAG(
    'treino-04',# Nome da DAG
    description='Utiliza os dados do ENADE para demonstrar o Paralelismo', # Descrição que facilita a identificação da DAG
    default_args=default_args,
    schedule_interval='*/10 * * * *'# Intervalo de execução utilizando cron
)

# Task que marca o inicio do processo
start_processing = BashOperator(
    task_id='start_processing',
    bash_command='echo "Starting Preprocessing! Vai!"',
    dag=dag
)


# Baixa os dados do ENADE 2019 do site oficial 
task_get_data = BashOperator(
    task_id='get_data',
    bash_command=f'wget -P /root/download http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o {data_path}/enade_2019.zip',
    dag=dag
)


def unzip_data():
    with zipfile.ZipFile(f'{data_path}/enade_2019.zip', 'r') as zipped:
        zipped.extractall(f'{data_path}')

# Task responsável pelo unzip do arquivo
task_unzip_data = PythonOperator(
    task_id = 'unzip_data',
    python_callable = unzip_data,
    dag=dag
)


def apply_filter():
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE', 'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
    
    enade = pd.read_csv(f'{data_path}/microdados_enade_2019/2019/3.DADOS/microdados_enade_2019.txt', sep=';', decimal=',', usecols=cols)

    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]

    enade.to_csv(data_path + '/enade_filtrado_2019.csv', index=False)


task_apply_filter = PythonOperator(
    task_id = 'apply_filter',
    python_callable = apply_filter,
    dag=dag 
)

# Idade centralizada na média
# Idade centralizada na média ao quadrado

def construct_centralized_age():
    age = pd.read_csv(f'{data_path}/enade_filtrado_2019.csv', usecols=['NU_IDADE'])

    age['centralized_age'] = age.NU_IDADE - age.NU_IDADE.mean()

    age[['centralized_age']].to_csv(data_path + '/centralized_age.csv', index=False)


def construct_centralized_pow():
    centralized_age = pd.read_csv(f'{data_path}/centralized_age.csv', sep=';', decimal=',')
    
    centralized_age['centralized_age'] = centralized_age['centralized_age'].astype(float)
    centralized_age['centralized_pow'] = centralized_age['centralized_age'] ** 2

    centralized_age[['centralized_pow']].to_csv(f'{data_path}/centralized_pow.csv', index=False)


task_construct_centralized_age = PythonOperator(
    task_id = 'centralized_age',
    python_callable = construct_centralized_age,
    dag=dag 
)

task_construct_centralized_pow = PythonOperator(
    task_id = 'centralized_pow',
    python_callable = construct_centralized_pow,
    dag=dag 
)


def construct_martial_status():
    filter = pd.read_csv(f'{data_path}/enade_filtrado_2019.csv', usecols=['QE_I01'])

    filter['martial_status'] = filter.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outro'
    })

    filter[['martial_status']].to_csv(f'{data_path}/martial_status.csv', index=False)



task_construct_martial_status = PythonOperator(
    task_id = 'construct_martial_status',
    python_callable = construct_martial_status,
    dag = dag
)


def construct_color():
    filter = pd.read_csv(f'{data_path}/enade_filtrado_2019.csv', usecols=['QE_I02'])

    filter['color'] = filter.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indígena',
        'F': '',
        ' ': ''
    })
    
    filter[['color']].to_csv(f'{data_path}/color.csv', index=False)

task_construct_color = PythonOperator(
    task_id='construct_color',
    python_callable = construct_color,
    dag = dag
)

def construct_escopai():
    filter = pd.read_csv(f'{data_path}/enade_filtrado_2019.csv', usecols=['QE_I04'])
    filter['escopai'] = filter.QE_I04.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5
    })
    filter[['escopai']].to_csv(f'{data_path}/escopai.csv', index=False)

task_construct_escopai = PythonOperator(
    task_id = 'construct_escopai',
    python_callable = construct_escopai,
    dag = dag
)

def construct_escomae():
    filter = pd.read_csv(f'{data_path}/enade_filtrado_2019.csv', usecols=['QE_I05'])
    
    filter['escomae'] = filter.QE_I05.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5
    })
    filter[['escomae']].to_csv(f'{data_path}/escomae.csv', index = False)

task_construct_escomae = PythonOperator(
    task_id = 'construct_escomae',
    python_callable = construct_escomae,
    dag = dag
)

def construct_renda():
    filter = pd.read_csv(f'{data_path}/enade_filtrado_2019.csv', usecols=['QE_I08'])
    filter['renda'] = filter.QE_I08.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5,
        'G': 6
    })
    filter[['renda']].to_csv(f'{data_path}/renda.csv', index = False)

task_construct_renda = PythonOperator(
    task_id = 'construct_renda',
    python_callable = construct_renda,
    dag = dag
)





################################## Task de JOIN ##########################################################

def join_data():
    filter = pd.read_csv(f'{data_path}/enade_filtrado_2019.csv', sep=';', decimal=',')
    centralized_age = pd.read_csv(f'{data_path}/centralized_age.csv', sep=';', decimal=',')
    centralized_pow = pd.read_csv(f'{data_path}/centralized_pow.csv', sep=';', decimal=',')
    martial_status = pd.read_csv(f'{data_path}/martial_status.csv', sep=';')
    color = pd.read_csv(f'{data_path}/color.csv', sep=';')
    escopai = pd.read_csv(f'{data_path}/escopai.csv', sep=';')
    escomae = pd.read_csv(f'{data_path}/escomae.csv', sep=';')
    renda = pd.read_csv(f'{data_path}/renda.csv', sep=';')

    final = pd.concat([
        filter, centralized_age, centralized_pow,
        martial_status, color, escomae, escopai, renda
    ],
        axis = 1
    )

    final.to_csv(f'{data_path}/enade_tratado.csv', index=False)

task_join = PythonOperator(
    task_id = 'join_data',
    python_callable = join_data,
    dag = dag
)

#### Task load ##########################

def load_data():
    enade = pd.read_csv(f'{data_path}/enade_tratado.csv')

    engine = sqlalchemy.create_engine(
        f'mssql+pyodbc://@localhost/enade?Integrated Security=True;driver=ODBC+Driver+17+for+SQL+Server'
    )

    enade.to_sql('tratado', con=engine, index=False, if_exists='append')

task_load_data = PythonOperator(
    task_id = 'load_data',
    python_callable = load_data,
    dag = dag
)

# Orquestração 

start_processing >> task_get_data >> task_unzip_data >> task_apply_filter
task_apply_filter >> [
    task_construct_centralized_age, task_construct_martial_status,
    task_construct_color, task_construct_escomae,
    task_construct_escopai, task_construct_renda
]

task_construct_centralized_pow.set_upstream(task_construct_centralized_age) # set_upstream() define que a task deve ser executada após a finalização da task indicada

task_join.set_upstream([
    task_construct_escomae, task_construct_escopai, task_construct_renda,
    task_construct_martial_status, task_construct_color, task_construct_centralized_pow # Como centralized_pow só pode ser concluído após a centralized_age a segunda não precisa aparecer nessa lista
])

task_join >> task_load_data