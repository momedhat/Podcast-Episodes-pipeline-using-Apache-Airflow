from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from SubTasksCode import getEpisodesRequests, download_episodes



def episodes_data(ti):
    episodes = getEpisodesRequests()
    print(f"Total {len(episodes)} episodes has found.")
    ti.xcom_push(key='episodes', value=episodes)
    
def load_episodes_data(ti):
        episodes = ti.xcom_pull(task_ids='get_episodes', key='episodes')

        hook = SqliteHook(sqlite_conn_id="podcasts")
        stored_episodes = hook.get_pandas_df("SELECT * FROM episodes;")
        new_episodes = []

        for episode in episodes:
            if episode['link'] not in stored_episodes['link'].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append((episode['link'], episode['title'], episode['pubDate'], episode['description'], filename))

        hook.insert_rows(table='episodes', rows=new_episodes, target_fields=['link', 'title', 'published', 'description', 'filename'])
        return new_episodes

def download_episodes_data(ti):
        episodes = ti.xcom_pull(task_ids='get_episodes', key='episodes')
        download_episodes(episodes)




default_args = {
    'owner': 'Mohamed Medhat',
    'start_date': datetime(2023, 10, 31)
}

with DAG (
    dag_id = "podcasts_summary",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False    
)as dag :
    
    # Task 1 : get episodes using requests
    get_episodes = PythonOperator(
        task_id = "get_episodes",
        python_callable = episodes_data,
    )

    # Task 2 : create sqlite database
    create_db = SqliteOperator(
        task_id="create_episodes_table",
        sql="""
            CREATE TABLE IF NOT EXISTS episodes (
                link TEXT PRIMARY KEY,
                title TEXT,
                filename TEXT,
                published TEXT,
                description TEXT
            )
        """,
        sqlite_conn_id="podcasts"
    )

    # Task 3 : load podcasts data into sqlite database
    load_episodes = PythonOperator(
        task_id = "load_episode_into_db",
        python_callable = load_episodes_data
    )

    # Task 4 : download podcasts
    download_podcasts = PythonOperator(
         task_id = "download_podcasts",
         python_callable = download_episodes_data

    )


    # dependencies instructions
    create_db >> get_episodes >> [load_episodes, download_podcasts]

    

