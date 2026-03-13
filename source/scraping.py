#Imoratation des bibliothéques
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os


# URL de départ
#base_site = "https://books.toscrape.com/"

livres = []
# Scraper les pages 10 pages
for page in range(1, 11):
    url = f"https://books.toscrape.com/catalogue/page-{page}.html"
    response = requests.get(url)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, "html.parser")
    
    for book in soup.select("article.product_pod"):
        title = book.h3.a["title"]
        price = book.select_one("p.price_color").text
        stock = book.select_one("p.instock.availability").text.strip()
        
        # Colonnes du fichiers csv ajouté métadonnée source
        livres.append({
            "ISBN": None,                 
            "title": title,
            "author": None,               
            "price": price,
            "category": None,             
            "availability": stock,
            "source": "scraping"
        })

# Convertir en DataFrame
df = pd.DataFrame(livres)


# Nettoyage des colonnes
#df["price"] = df["price"].str.replace(r"[^0-9.]", "", regex=True).astype(float)
#df["availability"] = df["availability"].str.strip()


# Créer le dossier data si nécessaire
os.makedirs("../data", exist_ok=True)
# Sauvegarde avec le type CSV prêt pour Spark
sortie = "../spark/data/livres_scraping.csv"
df.to_csv(sortie, index=False)
print(f"Scraping terminé. Fichier enregistré dans {sortie}")


# Vérification du contenu
print(df.head())
print(df.columns.tolist())
print("📦 Nombre total de lignes :", len(df))
