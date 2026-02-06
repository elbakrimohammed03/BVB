import requests
import pandas as pd
import re
import os
import urllib3
from bs4 import BeautifulSoup
from supabase import create_client

# Désactiver les alertes SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Vos coordonnées Supabase
SUPABASE_URL = "https://nbgpxasdgucltfcygqua.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5iZ3B4YXNkZ3VjbHRmY3lncXVhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAzMzc1NDAsImV4cCI6MjA4NTkxMzU0MH0.EpLaGobOZxa_VI-_cOBXoDBiB7J-5QaC9vNV4lyNNKc"

# Initialisation du client Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def clean_numeric(value):
    if not value or value.strip() in ['-', '']: return 0.0
    clean = re.sub(r'[^\d,.-]', '', value).replace(' ', '').replace(',', '.')
    try:
        return float(clean)
    except:
        return 0.0

def extraire_date_bourse(soup):
    texte_page = soup.get_text(separator=' ')
    mois_fr = {
        'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
        'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
        'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12'
    }
    pattern = r'(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})'
    match = re.search(pattern, texte_page, re.IGNORECASE)
    if match:
        jour = match.group(1).zfill(2)
        mois = mois_fr[match.group(2).lower()]
        annee = match.group(3)
        return f"{annee}-{mois}-{jour}"
    return None

def run_sync():
    url_bourse = "https://www.casablanca-bourse.com/fr/live-market/marche-actions-groupement"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(url_bourse, headers=headers, verify=False, timeout=30)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        date_iso = extraire_date_bourse(soup)
        if not date_iso:
            print("Erreur : Impossible de trouver la date sur la page.")
            return

        print(f"Tentative d'insertion pour la séance du : {date_iso}")
        rows = soup.select("table tbody tr")
        data_to_insert = []

        for row in rows:
            cells = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cells) < 16: continue
            
            # Mapping vers votre table SQL bourse_maroc
            ligne = {
                "seance": date_iso,
                "instrument": cells[0].strip(),
                "ouverture": clean_numeric(cells[3]),
                "dernier_cours": clean_numeric(cells[4]),
                "plus_haut": clean_numeric(cells[8]),
                "plus_bas": clean_numeric(cells[9]),
                "quantite_titres": clean_numeric(cells[5]),
                "volume_echanges": clean_numeric(cells[6]),
                "nb_contrats": int(clean_numeric(cells[15])),
                "capitalisation": clean_numeric(cells[14]),
                "cours_ajuste": clean_numeric(cells[4])
            }
            data_to_insert.append(ligne)

        if data_to_insert:
            # Insertion dans Supabase
            supabase.table("bourse_maroc").insert(data_to_insert).execute()
            print(f"Succès : {len(data_to_insert)} lignes insérées pour le {date_iso}.")
        
    except Exception as e:
        if "duplicate key value" in str(e):
            print(f"Info : La séance du {date_iso} existe déjà dans la base (Doublon bloqué).")
        else:
            print(f"Erreur : {e}")

if __name__ == "__main__":
    run_sync()
