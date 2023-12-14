### DOWNLOAD ###
#wget https://www.zensus2011.de/SharedDocs/Downloads/DE/Pressemitteilung/DemografischeGrunddaten/csv_Demographie_100m_Gitter.zip?__blob=publicationFile&v=2
#unzip data.zip

### CODE ###
import pandas as pd

data = {}

local_chunks_count = 0

for chunk in pd.read_csv("demographic.csv", sep=";", encoding="iso-8859-1", chunksize=3000000):
    local_chunks_count += 1
    df = pd.DataFrame(chunk)
    df.drop(columns="Gitter_ID_100m_neu", inplace=True)
    for row in df.to_numpy():
      id = row[0]
      key = row[1] + "_" + row[3]
      key = key.lower().replace(" ", "_").replace("(", "_").replace(")", "_").replace("__", "_").replace(",", "").replace(".", "").replace("/-in", "").replace("/-inne", "").replace("-", "_").replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("_insgesamt", "insgesamt").replace("___", "_").replace("<", "kleiner").replace(">", "groeßer").replace("seniorennen", "senioren").replace(":_", "_").replace(":", "_")
      if id in data:
        key_volume = key + "_volume"
        key_volume = key_volume.replace("__", "_")
        data[id][key_volume] = row[4]
        key_quality = key + "_quality"
        key_quality = key_quality.replace("__", "_")
        data[id][key_quality] = row[5]
      else:
        if local_chunks_count >= 9:
          cleaned_data = []
          for id_value,value in data.items():
            value["id"] = id_value
            cleaned_data.append(value)
          df = pd.DataFrame(cleaned_data)
          df.to_gbq("PROJECT.DATASET.demographic", project_id="PROJECT", if_exists="append", chunksize=None)
          cleaned_data.clear()
          data.clear()
          local_chunks_count = 0
        data[id] = {"decade": "01/01/2011"}
        key_volume = key + "_volume"
        key_volume = key_volume.replace("__", "_")
        data[id][key_volume] = row[4]
        key_quality = key + "_quality"
        key_quality = key_quality.replace("__", "_")
        data[id][key_quality] = row[5]

cleaned_data = []
for id_value,value in data.items():
  value["id"] = id_value
  cleaned_data.append(value)
df = pd.DataFrame(cleaned_data)
df.to_gbq("PROJECT.DATASET.demographic", project_id="PROJECT", if_exists="append", chunksize=None)
cleaned_data.clear()
data.clear()