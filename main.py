import os
import pandas as pd
import time, datetime
import re

from package.utils import get_club_urls, get_players_urls,save_data_to_csv,get_player_information
from package.utils import LEAGUE_URLS
from package.utils.scraping_data import get_player_information_from_json_api
from multiprocessing import Pool, cpu_count
import shutil


DS_NAME = "data"


# Vérifier si le dossier de données existe, sinon le créer
with open("point_reprise.txt", "r") as f:
    lines = [line.strip() for line in f if line.strip()]

# Lire le point de reprise pour le championnat et le club
reprise_url_championnat = lines[0] if lines else None
start_index_championnat = next((i for i, league in enumerate(LEAGUE_URLS) if league["url"] == reprise_url_championnat), 0)

reprise_url_club = lines[1] if len(lines) > 1 else None


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

        print(f"Ligne {target_line_index + 1} mise à jour.")

    except Exception as e:
        print(f"[ERREUR] : {e}")

def write_clubs_and_players_to_csv():
    for value in LEAGUE_URLS[start_index_championnat:]:
        # Recuperer l'ensemble des information des clubs de la ligue parcourue dans un dataframe
        url_league = value["url"]
        df_clubs = get_club_urls(url_league)
        # Sauvegarder les informations dans un fichier CSV
        if value["url"] != reprise_url_championnat:
            save_data_to_csv(df_clubs,f"{DS_NAME}/clubs.csv")
            replace_or_append_line_in_file("point_reprise.txt", value["url"], 0)
        
        if reprise_url_club in df_clubs["url"].values and reprise_url_club is not None:
            reprise_url_club = df_clubs[df_clubs["url"] == reprise_url_club].index[0] # pyright: ignore[reportUnboundVariable]
            reprise_url_club = None
        else:
            start_index_club = 0
        
        # Recuperer les informations concernant la liste des joueurs pour chaque club de la ligue parcourue
        for url_club in df_clubs.iloc[start_index_club:].url:
            replace_or_append_line_in_file("point_reprise.txt", url_club, 1)
            df_players = get_players_urls(url_club)
            save_data_to_csv(df_players,f"{DS_NAME}/players.csv")
            
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
        print(f"🧹 Dossier temporaire supprimé : {path}")

if __name__ == '__main__':
    write_players_info_to_csv()
        
