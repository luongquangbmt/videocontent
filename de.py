import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import csv
import pyarrow.parquet as pq

# 1 : lire les fichiers "netflix_titles_nov_2019.csv" et "golden_globe_awards.csv" et afficher leurs schemas
# Lire les fichiers
print(
    'Ici, je présume que les films Netflix sont les films qui sont émis par Netflix. Ils ne sont pas les films produits par Netflix.')
print('1. Lire les fichiers.\n')
netflix_titles = csv.read_csv('netflix_titles_nov_2019.csv')
globe_awards = csv.read_csv('golden_globe_awards.csv')

# Afficher leurs schema
print('2. Afficher leurs schema.')
print('netflix_titles table schema\n', netflix_titles.schema, '\n')
print('globe awards table schema\n', globe_awards.schema, '\n')

#2 : transformer le format de la date de la colonne "date_added" dans le format suivant "yyyy-MM"

# Convertir les tables pyarrow à pandas dataframe
netflix_df = netflix_titles.to_pandas()
globe_df = globe_awards.to_pandas()

# Changer le type de la colonne "date_added" à datetime
netflix_df['date_added'] = pd.to_datetime(netflix_df['date_added'])

# Créer une autre colonne "date_added_yymm" avec le format "yyyy-MM'.
netflix_df['date_added_yymm'] = netflix_df['date_added'].dt.strftime('%Y-%m')

#3 : mettre la valeur "no data" dans la colonne "country" quand la valeur est vide ou null

netflix_df['country'].fillna('no data', inplace=True)
netflix_df['country'].replace(r'^\s*$', 'no data', regex=True, inplace=True)

print('e nombre des lignes qui n’ont aucunes données dans la colonne "country".', \
      netflix_df[netflix_df['country'] == 'no data'].count()[0])

#4 : extraire les films netflix qui on gagné un prix golden globe avec cette liste de colonnes ['film', 'year_film', 'category']
# generer un fichier resultat format JSON

# Essuyer les colonnes "film","title" et créer une nouvelle colonne "clean_film", "clean title"
stopword = ['The', 'the', '\,']
globe_df['clean_film'] = globe_df['film'].str.title().str.strip('').str.replace(',', '') \
    .apply(lambda x: ' '.join([item for item in x.split() if item not in stopword]))
netflix_df['clean_title'] = netflix_df['title'].str.title().str.replace(',', '') \
    .apply(lambda x: ' '.join([item for item in x.split() if item not in stopword]))

# Créer une nouvelle dataframe avec les films qui ont gagné des globe awards.
gagne_globe_df = globe_df[['film', 'clean_film', 'year_film', 'year_award', 'nominee', 'category']][
    globe_df['win'] == True]
print('le nombre des films qui ont gagné des prix :', gagne_globe_df.count()[0])

# Faire une join intersection de la  ataframe gagne_globe_df  avec netflix_df afin de produire un dataframe qui montre les films Netflix qui ont gagné des prix
netflix_globe_awards = pd.merge(netflix_df[['clean_title', 'title']], gagne_globe_df, \
                                how='inner', left_on='clean_title', right_on='clean_film') \
    .drop(columns=['clean_film', 'clean_title', 'film']).rename(columns={'title': 'film'})

print('le nombre des prix gagnés par les films Netflix:', netflix_globe_awards.count()[0])
print('le nombre des films Netflix qui ont gagné des prix:',
      netflix_globe_awards[['film']].drop_duplicates().count()[0])
print('\n')

# Exporter le dataframe à une fiche JSON
json_f = (netflix_globe_awards.drop(columns=['year_award', 'nominee'])
          .groupby(['film'])['year_film', 'category'] \
          .apply(lambda x: x.to_dict('r'))
          .to_json('netflix_globe_award.json'))

#5: extraire les films du fichier netflix qui ont un acteur qui a gagné un golden globe avec cette liste de colonnes ['actor_name', 'netflix_movie', 'gg_movie', 'year_award']
#  (Faites attention la colonne "cast" dans le fichier netflix est une colonne csv ex: "acteur1, acteur2, acteur3")
# generer un fichier resultat format parquet partitionné par la colonne 'year_award

# Créer une liste des acteurs qui ont gagné un prix.
# J'utilise dictionary parce que il est plus vite quand on itérer les colones.
gagnee_acteurs = gagne_globe_df.rename(columns={'film': 'gg_movie', 'nominee': 'actor_name'}) \
    .groupby(['actor_name'])['year_award', 'gg_movie'] \
    .apply(lambda x: x.to_dict('r')) \
    .reset_index(name='gg_info')

# Creér une nouvelle dataframe avec les films qui ont pour vedette des acteurs qui ont gagné un prix.
# Former une liste des acteurs figurant dans les films émis par Netflix et qui ont gagné des prix mondiaux.
netflix_temp_df = netflix_df.copy()
netflix_temp_df['casts'] = netflix_temp_df['cast'].apply(lambda x: x.split(', '))
netflix_temp_df['actors_with_gg'] = netflix_temp_df['casts'].apply(
    lambda x: set(x) & set(gagnee_acteurs['actor_name'].to_list()))
netflix_temp_df['actors_with_gg_s'] = [','.join(map(str, l)) for l in netflix_temp_df['actors_with_gg']]

# Enlever tous les films qui n’ont pas de vedette qui a  gagné un prix
l = list(netflix_temp_df.loc[netflix_temp_df['actors_with_gg_s'] == ''].index)
netflix_temp_df = netflix_temp_df.drop(l, axis=0)

# Creér une nouvelle dataframe globe_gagner_en_netflix
temp_df = netflix_temp_df.copy()
globe_gagner_en_netflix = pd.DataFrame()
for actors in temp_df.actors_with_gg_s:
    ind = temp_df.loc[temp_df['actors_with_gg_s'] == actors].index
    for actor in actors.split(','):
        temp_df2 = pd.DataFrame({'actor_name': actor, 'netflix_movie': temp_df.title[ind]})
        globe_gagner_en_netflix = globe_gagner_en_netflix.append(temp_df2, ignore_index=True)
globe_gagner_en_netflix = globe_gagner_en_netflix.groupby(['actor_name'])['netflix_movie'] \
    .apply(lambda x: x.to_list()) \
    .reset_index(name='netflix_movie')

# Connecter la liste des acteurs qui ont gagné des prix avec celle des films Netflix qui ont pour vedette des acteurs qui ont gagné un prix
final_df = pd.merge(globe_gagner_en_netflix, gagnee_acteurs, how='inner', on='actor_name')

final_df_pdformat = pd.DataFrame()
for ind in final_df.index:
    for gg_info in final_df['gg_info'][ind]:
        temp_df3 = pd.DataFrame({'actor_name': [final_df['actor_name'][ind]], \
                                 'netflix_movie': str(final_df['netflix_movie'][ind]), \
                                 'year_award': gg_info['year_award'], 'gg_movie': gg_info['gg_movie']})
        final_df_pdformat = final_df_pdformat.append(temp_df3, ignore_index=True)

print("Le dataframe ", final_df_pdformat.head(5))

# Exporter la trame de données à une fiche parquet
table = pa.Table.from_pandas(final_df_pdformat, preserve_index=False)
pq.write_to_dataset(table, root_path='netflix_gg', partition_cols=['year_award'])
