### DOWNLOAD ###
#wget https://www.zensus2011.de/SharedDocs/Downloads/DE/Pressemitteilung/DemografischeGrunddaten/csv_Bevoelkerung_100m_Gitter.zip?__blob=publicationFile&v=2
#unzip data.zip

### CODE ###
import pandas as pd

df = pd.read_csv("population.csv", sep=";", encoding="iso-8859-1")

df.drop(columns=["x_mp_100m", "y_mp_100m"], inplace=True)
df.rename(columns={"Gitter_ID_100m": "id", "Einwohner": "population_volume"}, inplace=True)
df = df[df["population_volume"] != -1]
df["decade"] = "01/01/2011"

df.to_gbq("PROJECT.DATASET.population", project_id="PROJECT", if_exists="replace", chunksize=None)