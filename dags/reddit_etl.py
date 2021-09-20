import json
import pandas as pd
import praw
from sqlalchemy import create_engine
from collections import Counter


def parse_wm_de(title: str) -> str:
    '''Retorna el nombre del Escritorio/Gestor de ventanas'''
    title = title.split()
    wm_de = title[0][1:len(title[0])-1]
    return wm_de


def check_if_valid_data(df: pd.DataFrame) -> bool:
    '''Revisa que los datos sean válidos.'''
    # Dataframe vacío
    if df.empty:
        print('Sin datos.')
        return False

    # Unicidad de llaves primarias
    if pd.Series(df['id']).is_unique:
        pass
    else:
        raise Exception('Llave primaria repetida')

    return True


def run_reddit_etl() -> None:
    # Autenticación de Reddit:
    # 1. Ingresa a https://www.reddit.com/prefs/apps.
    # 2. Selecciona “script” como el tipo de la app.
    # 3. Ingresa un nombre y una descripción.
    # 4. Establece el redirect uri en http://localhost:8080.
    # Una vez tengas client_id y client_secret, ejecuta
    # refreshtoken.py, te pedirá estos datos y los scopes,
    # ingresa all para esta última parte.
    # Nota: el script refreshtoken.py así como los pasos fueron
    # obtenidos desde https://www.jcchouinard.com/get-reddit-api-credentials-with-praw/
    # no pretendo clamar la autoría del script.

    # Por cuestiones de seguridad, las credenciales
    # se guardan en un archivo externo

    FILENAME = 'client_secrets.json'
    DATABASE_LOCATION = 'sqlite:///reddit_dataset.db'

    try:
        with open(FILENAME, 'r') as f:
            creds = json.load(f)
    except FileNotFoundError:
        exit(f'no se encontró el archivo {FILENAME}, fin de la ejecución')

    reddit = praw.Reddit(
        client_id=creds.get('client_id'),
        client_secret=creds.get('client_secret'),
        user_agent=creds.get('user_agent'),
        redirect_uri=creds.get('redirect_uri'),
        refresh_token=creds.get('refresh_token')
    )

    # Datos que serán scrapeados
    id_list = []
    title_list = []
    score_list = []
    upvote_ratio_list = []
    link_flair_text_list = []

    # Subreddit que será scrapeado
    subreddit = reddit.subreddit('unixporn')
    hot_post = subreddit.hot(limit=10000)
    for sub in hot_post:
        id_list.append(sub.id)
        link_flair_text_list.append(sub.link_flair_text)
        score_list.append(sub.score)
        title_list.append(sub.title)
        upvote_ratio_list.append(sub.upvote_ratio)

    print(subreddit, 'completado; ', end='')
    print('total', len(id_list), 'posts han sido scrapeados')

    # Se crea el dataframe
    df = pd.DataFrame(
        {'id': id_list,
         'title': title_list,
         'upvote_count': score_list,
         'upvote_ratio': upvote_ratio_list,
         'flair': link_flair_text_list
         })

    # Filtrar post por el tag 'Screeshot'
    df = df.loc[df['flair'] == 'Screenshot']
    # Añadir esa nueva columna al dataframe
    def get_de(x): return parse_wm_de(x)
    df['desktop'] = df['title'].apply(get_de)
    # Nuevo data frame con las menciones de cada Escritorio/WM
    count_desktops = Counter(df['desktop']).most_common(20)
    desktop_mentions = pd.DataFrame(count_desktops)

    # Validar
    if check_if_valid_data(df):
        print('Datos válidos. Inicia proceso Load')
    else:
        exit('No son datos válidos')
    # Load
    engine = create_engine(DATABASE_LOCATION, echo=False)
    try:
        with engine.begin() as connection:
            df.to_sql('unixporn', con=connection,
                      if_exists='append', index=False)
            desktop_mentions.to_sql('desktop_mentions', con=connection,
                      if_exists='append', index=False)
        print('Bases de datos actualizadas')
    except Exception as e:
        exit(
            f'Error: base de datos no actualizada. Razón {e.with_traceback(None)}')


if __name__ == '__main__':
    run_reddit_etl()
