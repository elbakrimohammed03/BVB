import requests
import pandas as pd
import re
import os
import urllib3
from bs4 import BeautifulSoup
from supabase import create_client

# Désactiver les alertes de sécurité SSL pour les requêtes vers la bourse
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration Supabase via les Secrets GitHub
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def clean_numeric(value):
    """Nettoie les valeurs textuelles pour les convertir en float/int."""
    if not value or value.strip() in ['-', '']: return 0.0
    clean = re.sub(r'[^\d,.-]', '', value).replace(' ', '').replace(',', '.')
    try:
        return float(clean)
    except:
        return 0.0

def extraire_date_bourse(soup):
    """Extrait et convertit la date du site au format YYYY-MM-DD."""
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
            print("Erreur : Date de séance introuvable.")
            return

        print(f"Séance détectée : {date_iso}")
        rows = soup.select("table tbody tr")
        data_to_insert = []

        for row in rows:
            cells = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cells) < 16: continue
            
            # Mapping vers les colonnes de votre table Supabase bourse_maroc
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
                "cours_ajuste": clean_numeric(cells[4]) # Utilise le dernier cours par défaut
            }
            data_to_insert.append(ligne)

        if data_to_insert:
            # Envoi vers Supabase
            # La contrainte UNIQUE (seance, instrument) empêchera les doublons
            result = supabase.table("bourse_maroc").insert(data_to_insert).execute()
            print(f"Succès : {len(data_to_insert)} instruments mis à jour sur Supabase.")
        
    except Exception as e:
        print(f"Erreur lors de la synchronisation : {e}")

if __name__ == "__main__":
    run_sync()
