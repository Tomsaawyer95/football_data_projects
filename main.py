import os
import pandas as pd
import time, datetime
import re

from package.utils import get_club_urls, get_players_urls,save_data_to_csv,get_player_information
from package.utils import LEAGUE_URLS
from package.utils.scraping_data import get_player_information_from_json_api
from multiprocessing import Pool, cpu_count
import shutil
import random


DS_NAME = "data"

# Vérifier si le dossier de données existe, sinon le créer
with open("point_reprise.txt", "r") as f:
    lines = [line.strip() for line in f if line.strip()]

# Lire le point de reprise pour le championnat et le club
reprise_url_championnat = lines[0] if lines else None
start_index_championnat = next((i for i, league in enumerate(LEAGUE_URLS) if league["url"] == reprise_url_championnat), 0)

reprise_url_club = lines[1] if len(lines) > 1 else None
reprise_year = lines[2] if len(lines) > 2 else None


def replace_or_append_line_in_file(file_path, new_line_content, target_line_index):
    """
    Remplace la ligne à l'index donné si elle existe, sinon ajoute la ligne à la fin.

    Args:
        file_path (str): Chemin du fichier texte.
        new_line_content (str): Nouveau contenu (sans \n).
        target_line_index (int): Index de la ligne à modifier (0 = première ligne).
    """
    try:
        # Lire les lignes existantes (ou vide si le fichier n'existe pas encore)
        try:
            with open(file_path, "r") as f:
                lines = f.readlines()
        except FileNotFoundError:
            lines = []

        # Nettoyage du contenu
        new_line = new_line_content.strip() + "\n"

        # Remplacer si l'index est valide
        if target_line_index < len(lines):
            lines[target_line_index] = new_line
        else:
            # Compléter avec des lignes vides si nécessaire
            while len(lines) < target_line_index:
                lines.append("\n")
            lines.append(new_line)

        # Réécriture
        with open(file_path, "w") as f:
            f.writelines(lines)

    except Exception as e:
        print(f"[ERREUR] : {e}")

def write_clubs_and_players_to_csv(year=None, reprise_url_club=None, championnat_start_index=0):
    
    # Variable locale pour gérer la reprise du club
    current_reprise_url_club = reprise_url_club
    
    for value in LEAGUE_URLS[championnat_start_index:]:
        # Recuperer l'ensemble des information des clubs de la ligue parcourue dans un dataframe
        if year:
            url_league = value["url"] + f"/plus/?saison_id={year}"
        else :
            url_league = value["url"]
        df_clubs = get_club_urls(url_league, year=year)
        
        # Vérifier que le DataFrame des clubs n'est pas vide
        if df_clubs.empty:
            print(f"Aucun club trouvé pour {value['name']} en {year}. Passage au championnat suivant.")
            continue
        
        # Sauvegarder les informations dans un fichier CSV
        if championnat_start_index == 0:
            if year:
                save_data_to_csv(df_clubs,f"{DS_NAME}/{year}/clubs_{year}.csv")
            else:
                save_data_to_csv(df_clubs,f"{DS_NAME}/clubs.csv")
            replace_or_append_line_in_file("point_reprise.txt", value["url"], 0)

        # Déterminer l'index de départ pour les clubs
        if current_reprise_url_club is not None and current_reprise_url_club in df_clubs["url"].values:
            start_index_club = df_clubs[df_clubs["url"] == current_reprise_url_club].index[0]
            current_reprise_url_club = None  # Réinitialiser après utilisation
        else:
            start_index_club = 0
        
        # Vérifier que l'index est valide et qu'il y a des clubs à traiter
        if start_index_club >= len(df_clubs):
            print(f"Index de départ ({start_index_club}) dépasse le nombre de clubs ({len(df_clubs)}). Passage au championnat suivant.")
            continue
            
        clubs_to_process = df_clubs.iloc[start_index_club:]
        if clubs_to_process.empty:
            print(f"Aucun club à traiter pour ce championnat. Passage au suivant.")
            continue
        
        # Recuperer les informations concernant la liste des joueurs pour chaque club de la ligue parcourue
        for url_club in clubs_to_process.url:
            replace_or_append_line_in_file("point_reprise.txt", url_club, 1)
            df_players = get_players_urls(url_club)
            
            # Vérifier que le DataFrame des joueurs n'est pas vide avant de sauvegarder
            if not df_players.empty:
                if year:
                    save_data_to_csv(df_players, f"{DS_NAME}/{year}/players_{year}.csv")
                else:
                    save_data_to_csv(df_players, f"{DS_NAME}/players.csv")
            else:
                print(f"Aucun joueur trouvé pour le club : {url_club}")


def write_players_info_to_csv():

    num_workers = min(4, cpu_count())  
    df_players = pd.read_csv(f"{DS_NAME}/players.csv")
    list_players = df_players["player_id"]
    
    with Pool(processes=num_workers) as pool:
        pool.map(get_player_information_from_json_api, list_players)
    safe_stamp = datetime.datetime.now().strftime("%Y%m%d") 
    merge_temp_jsonl_files(output_path=f"data/players_info_{safe_stamp}.jsonl", input_path_file="players_part_")
    merge_temp_jsonl_files(output_path=f"data/transfert_info_{safe_stamp}.jsonl", input_path_file="transfert_part_")
    cleanup_temp_dir()
        
def merge_temp_jsonl_files(output_path, input_path_file):
    os.remove(output_path) if os.path.exists(output_path) else None
    with open(output_path, "w", encoding="utf-8") as fout:
        for fname in os.listdir("data/temp"):
            if fname.startswith(input_path_file) and fname.endswith(".jsonl"):
                with open(os.path.join("data/temp", fname), "r", encoding="utf-8") as f:
                    for line in f:
                        fout.write(line)
    
def cleanup_temp_dir(path="data/temp"):
    if os.path.exists(path):
        shutil.rmtree(path)

if __name__ == '__main__':
    print("Démarrage du script de récupération des données de football...")
    debut = 2014
    start_year = max(int(reprise_year), debut) if reprise_year else debut
    reprise = True
    
    for year in range(start_year, 2024):
        replace_or_append_line_in_file("point_reprise.txt", str(year), 2)
        if reprise:
            current_reprise_url_club = reprise_url_club
            current_championnat_index = start_index_championnat
            reprise = False  # Réinitialiser la reprise après la première itération
        else:
            current_reprise_url_club = None
            current_championnat_index = 0
            # Réinitialiser le point de reprise du championnat pour la nouvelle année
        write_clubs_and_players_to_csv(year, reprise_url_club=current_reprise_url_club, championnat_start_index=current_championnat_index)
        replace_or_append_line_in_file("point_reprise.txt", "", 0)  # Pas de club de reprise
        replace_or_append_line_in_file("point_reprise.txt", "", 1)  # Pas de club de reprise
        
        
        
