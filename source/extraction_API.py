# Importation des bibliothèques nécessaires
import requests
import pandas as pd
import os

# Création du dossier data dans le dossier spark
data_dir = "../spark/data/api_livres/"
os.makedirs(data_dir, exist_ok=True)

# Appel API OpenLibrary
url = "https://openlibrary.org/search.json?q=python&limit=1000"
response = requests.get(url)
api_data = response.json()

livres = []
for doc in api_data.get('docs', []):
    livres.append({
        "ISBN": doc.get("isbn")[0] if doc.get("isbn") else None,
        "title": doc.get("title"),
        "author": ", ".join(doc.get("author_name", [])) if doc.get("author_name") else None,
        "price": None,
        "category": None,
        "availability": None,
        "source": "API openlibrary.org"
    })

# Convertir la liste livres en DataFrame Pandas
df = pd.DataFrame(livres)

# Sauvegarde sous forme CSV prêt à utiliser sur Spark
file_path = os.path.join(data_dir, "api_livres.csv")
df.to_csv(file_path, index=False)

print(f"✅ Extraction API OpenLibrary terminée, fichier créé : {file_path}")
print("📊 Aperçu des données :")
print(df.head())
print(df.columns.tolist())
print("📦 Nombre total de lignes :", len(df))