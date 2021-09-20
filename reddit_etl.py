import json
import pandas as pd
import praw
from sqlalchemy import create_engine

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Dataframe vacío
    if df.empty:
        print("Sin datos. Fin de la ejecución")
        return False 

    # Unicidad de llaves primarias
    if pd.Series(df['ID']).is_unique:
        pass
    else:
        raise Exception("Llave primaria repetida")

    # Valores nulos
    if df.isnull().values.any():
        raise Exception("Valores nulos encontrados")

    return True

def run_reddit_etl():
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
    filename = 'client_secrets.json'
    try:
        with open(filename, 'r') as f:
            creds = json.load(f)
    except FileNotFoundError:
        exit(f'no se encontró el archivo {filename}, fin de la ejecución')

    reddit = praw.Reddit(   
        client_id=creds.get('client_id'),
        client_secret=creds.get('client_secret'),
        user_agent=creds.get('user_agent'),
        redirect_uri=creds.get('redirect_uri'),
        refresh_token=creds.get('refresh_token')
    )

    DATABASE_LOCATION = 'sqlite:///reddit_dataset.db'

    # Datos que serán scrapeados
    author_list = []
    id_list = []
    link_flair_text_list = []
    num_comments_list = []
    score_list = []
    title_list = []
    upvote_ratio_list = []

    # Subreddit que será scrapeado
    subreddit = reddit.subreddit('unixporn')
    hot_post = subreddit.hot(limit=10000)
    for sub in hot_post:
        author_list.append(sub.author)
        id_list.append(sub.id)
        link_flair_text_list.append(sub.link_flair_text)
        num_comments_list.append(sub.num_comments)
        score_list.append(sub.score)
        title_list.append(sub.title)
        upvote_ratio_list.append(sub.upvote_ratio)
    
    print(subreddit, 'completado; ', end='')
    print('total', len(author_list), 'posts han sido scrapeados')
    
    # Obtención del dataframe
    df = pd.DataFrame({'ID':id_list, 
                   'Author':author_list, 
                   'Title':title_list,
                   'Count_of_Comments':num_comments_list,
                   'Upvote_Count':score_list,
                   'Upvote_Ratio':upvote_ratio_list,
                   'Flair':link_flair_text_list
                  })

    # Validar
    if check_if_valid_data(df):
        print('Datos válidos. Inicia proceso Load')
    
    # Load
    engine = create_engine(DATABASE_LOCATION, echo=False)
    try:
        with engine.begin() as connection:
            df.to_sql('unixporn', con=connection, if_exists='append', index=False)
    except:
        print(f'Datos existentes en la base de datos {DATABASE_LOCATION}')
    
    print("Base de datos actualizada")

