import pandas as pd

# Chemin CSV initial
chemin_csv = "../spark/data/livres_externe.csv"

# Lecture
df = pd.read_csv(chemin_csv)

# Colonnes utiles et renommage
colonnes = {
    "ISBN": "ISBN",
    "Book-Title": "title",
    "Book-Author": "author"
}
df = df[list(colonnes.keys())].rename(columns=colonnes)

# Ajouter colonnes manquantes
df["price"] = None
df["category"] = None
df["availability"] = None
df["source"] = "csv"

# Réordonner selon le schéma final
df = df[["ISBN", "title", "author", "price", "category", "availability", "source"]]

# Écrire un CSV propre pour Spark
df.to_csv("../spark/data/livres_externe_clean.csv", index=False)

#Verification du contenu
print("✅ CSV externe propre créé")
print(df.head())
print(df.columns.tolist())
print("📦 Nombre total de lignes :", len(df))
