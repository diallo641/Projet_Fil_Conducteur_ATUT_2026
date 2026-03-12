import requests
import pandas as pd
import os


# Création du dossier
data_dir = "../spark/data/api_books/"
os.makedirs(data_dir, exist_ok=True)

# ----------------------------
# Appel API OpenLibrary
# ----------------------------
# Exemple: récupérer des livres sur "Python"
url = "https://openlibrary.org/search.json?q=python&limit=50"
response = requests.get(url)
data = response.json()

books = []
for doc in data.get('docs', []):
    books.append({
        "ISBN": doc.get("isbn")[0] if doc.get("isbn") else None,     # ISBN si disponible
        "title": doc.get("title"),
        "author": ", ".join(doc.get("author_name", [])) if doc.get("author_name") else None,
        "price": None,           # Pas disponible côté API
        "category": None,        # Pas disponible côté API
        "availability": None,    # Pas disponible côté API
        "source": "API openlibrary.org"
    })

# ----------------------------
# Convertir en DataFrame Pandas
# ----------------------------
df = pd.DataFrame(books)

# ----------------------------
# Sauvegarde CSV prêt pour Spark
# ----------------------------
file_path = os.path.join(data_dir, "api_books.csv")
df.to_csv(file_path, index=False)

print(f"✅ Extraction API terminée, fichier créé : {file_path}")
print("📊 Aperçu des données :")
print(df.head())
print(df.columns.tolist())
