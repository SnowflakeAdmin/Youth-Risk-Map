### DOWNLOAD & INSTALL ###
#wget https://daten.gdz.bkg.bund.de/produkte/sonstige/geogitter/aktuell/DE_Grid_ETRS89-UTM32_100m.csv.zip
#unzip data.zip
#wget https://www.riserid.eu/data/user_upload/downloads/info-pdf.s/Diverses/Liste-Amtlicher-Gemeindeschluessel-AGS-2015.pdf
#pip install tabula-py

### CODE ###
import tabula
import json

df = tabula.read_pdf("Liste-Amtlicher-Gemeindeschluessel-AGS-2015.pdf", pages="all")

data_master = {}

for i in df[1:]:
  cols = i.columns
  district_id = str(cols[2])
  district_id = district_id if len(district_id) == 8 else (8 - len(district_id)) * "0" + district_id
  data_master[district_id] = {
        "district_name": cols[0],
        "district_type": cols[1],
        "federal_state": cols[3]
    }
  for row in i.to_numpy():
    district_id = str(row[2])
    district_id = district_id if len(district_id) == 8 else (8 - len(district_id)) * "0" + district_id
    data_master[district_id] = {
      "district_name": row[0],
      "district_type": row[1],
      "federal_state": row[3]
    }

for row in df[0].to_numpy():
  district_id = str(row[3])
  district_id = district_id if len(district_id) == 8 else (8 - len(district_id)) * "0" + district_id
  data_master[district_id] = {
        "district_name": row[0],
        "district_type": row[2],
        "federal_state": row[4]
  }

with open("district_encoding.json", "w") as file:
  json.dump(data_master, file)

import pandas as pd
from pyproj import Transformer
from concurrent.futures import ProcessPoolExecutor
import os

with open("district_encoding.json", "r") as file:
  district_encoding = json.load(file)

transformer = Transformer.from_crs("epsg:3035", "epsg:4326", always_xy=True)

def transform_row(row):
    global transformer, district_encoding
    lon, lat = transformer.transform(float(row[3]), float(row[4]))
    district_id = str(row[-1])
    if district_id != "-":
      district_id = district_id if len(district_id) == 8 else (8 - len(district_id)) * "0" + district_id
    try:
      district_name = district_encoding[district_id]["district_name"] if district_id != "-" else "Not clearly assignable."
      district_type = district_encoding[district_id]["district_type"] if district_id != "-" else "Not clearly assignable."
      federal_state_name = district_encoding[district_id]["federal_state"] if district_id != "-" else "Not clearly assignable."
    except KeyError:
      print(f"[WARN]: Not found id ---> {district_id}")
      district_name = "No reference found."
      district_type = "No reference found."
      federal_state_name = "No reference found."
    return {
        "id": row[0],
        "lon": lon,
        "lat": lat,
        "district_name": district_name,
        "district_type": district_type,
        "federal_state": federal_state_name
    }

data = []

file_paths = [os.path.join("data", file_name) for file_name in os.listdir("data")]

for file_path in file_paths:
    print(f"[INFO]: Now in process ---> {file_path}")
    df = pd.read_csv(file_path, sep=";", low_memory=False)
    rows = df.to_numpy()
    num_cores = 8
    chunk_size = len(rows) // num_cores
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        results = list(executor.map(transform_row, rows, chunksize=chunk_size))
    data.extend(results)

df = pd.DataFrame(data)
df.to_gbq("PROJECT.DATASET.etrs89_encoding", project_id="PROJECT", if_exists="replace", chunksize=None)