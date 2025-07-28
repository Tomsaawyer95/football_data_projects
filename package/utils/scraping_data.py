import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import json
from multiprocessing import current_process

LEAGUE_URLS = [
    {"name": "Ligue 1", "url": "https://www.transfermarkt.com/ligue-1/startseite/wettbewerb/FR1"},
    {"name": "Ligue 2", "url": "https://www.transfermarkt.com/ligue-2/startseite/wettbewerb/FR2"},
    {"name": "Serie A", "url": "https://www.transfermarkt.com/serie-a/startseite/wettbewerb/IT1"},
    {"name": "Serie B", "url": "https://www.transfermarkt.com/serie-b/startseite/wettbewerb/IT2"},
    {"name": "Bundesliga", "url": "https://www.transfermarkt.com/bundesliga/startseite/wettbewerb/L1"},
    {"name": "2. Bundesliga", "url": "https://www.transfermarkt.com/2-bundesliga/startseite/wettbewerb/L2"},
    {"name": "Premier League", "url": "https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1"},
    {"name": "Championship", "url": "https://www.transfermarkt.com/championship/startseite/wettbewerb/GB2"},
    {"name": "La Liga", "url": "https://www.transfermarkt.com/laliga/startseite/wettbewerb/ES1"},
    {"name": "La Liga 2", "url": "https://www.transfermarkt.com/laliga2/startseite/wettbewerb/ES2"},
]

OUTPUT_DIR = "data/temp"
    
def get_html_body_from_url(url: str):
    """
    Récupère le contenu HTML d'une page.
    Args:
        url (str): URL de la page.
    Returns:
        str: Contenu HTML de la page.
    """
    headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "referer": "https://www.transfermarkt.com/",
    "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
}
    
    session = requests.Session()
    session.headers.update(headers)
    #session.proxies.update({"http": random.choice(proxies), "https": random.choice(proxies)})
    
    for attempt in range(3):
        try:
            response = session.get(url, timeout=10)
            if response.status_code == 200:
                break
        except requests.RequestException as e:
            print(f"Erreur de connexion pour {url}: {e}")
            if attempt < 2:
                time.sleep(60)
            elif response.status_code == 503:
                print("503 reçu. Le serveur est indisponible.")
            else:
                print(f"Erreur HTTP : {response.status_code}")
        
        if attempt < 2:
            print("Nouvelle tentative dans 60s...")
            time.sleep(60)

    return BeautifulSoup(response.text, "html.parser")
    
    
def get_club_urls(url: str):
    """
    Récupère les URLs des clubs depuis Transfermarkt.
    Args:
        url (str): URL de la page des ligues.
        exemple: "https://www.transfermarkt.com/ligue-1/startseite/wettbewerb/FR1"
    Returns:
        pd.DataFrame: DataFrame contenant les noms et URLs des clubs.
    """ 

    soup = get_html_body_from_url(url)

    # Récupérer tous les liens vers les clubs
    club_table_summary = soup.find("div", id= "yw1")
    club_rows = club_table_summary.select("table.items tbody tr")

    clubs = []
    for row in club_rows:
        link = row.select_one("td.hauptlink a")
        if link:
            club_name = link.text.strip()
            club_url = "https://www.transfermarkt.com" + "/".join(link["href"].split("/")[0:5])
            clubs.append({"name": club_name, "url": club_url})

    df = pd.DataFrame(clubs)
    return df

def get_players_urls(club_url: str):
    """
    Récupère les URLs des joueurs d'un club depuis Transfermarkt.
    Args:
        club_url (str): URL du club.
        exemple: "https://www.transfermarkt.com/club-name/startseite/verein/1234"
    Returns:
        pd.DataFrame: DataFrame contenant les noms et URLs des joueurs.
    """
    soup = get_html_body_from_url(club_url)
    
    # Récupérer tous les liens vers les joueurs
    player_table_summary = soup.find("div", id="yw1")
    player_rows = player_table_summary.select("table.items tbody > tr")
    
    players = []
    for row in player_rows:
        link = row.select_one("td.hauptlink a")
        if link:
            player_id = link["href"].split("/")[-1]
            player_name = link.text.strip()
            player_url = "https://www.transfermarkt.com" + "/".join(link["href"].split("/")[0:5])
            players.append({"player_id" : player_id,"name": player_name, "url": player_url})

    df = pd.DataFrame(players)
    return df




############# RECUPERATION DES INFORAMTIONS DEPUIS L'API JSON #############

def get_player_information_from_json_api(player_id: int):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    process_id = current_process().pid
    player_info =  _extract_information_player_from_JSON_api(player_id)
    transfer_info = _extract_information_transfert_from_JSON_api(player_id)
    
    # Écriture ligne par ligne
    output_file = os.path.join(OUTPUT_DIR, f"players_part_{process_id}.jsonl")
    with open(output_file, "a", encoding="utf-8") as f:
        json.dump(player_info, f, ensure_ascii=False)
        f.write("\n")
        
    # Écriture ligne par ligne
    output_file = os.path.join(OUTPUT_DIR, f"transfert_part_{process_id}.jsonl")
    with open(output_file, "a", encoding="utf-8") as f:
        json.dump(transfer_info, f, ensure_ascii=False)
        f.write("\n")
        
    

def _extract_information_player_from_JSON_api(player_id: int):
    """
    Récupère les informations d'un joueur à partir de l'API JSON de Transfermarkt.
    Args:
        player_id (str): ID du joueur.
    Returns:
        dict: Dictionnaire contenant les informations du joueur.
    """
    url = f"https://tmapi-alpha.transfermarkt.technology/players?ids[]={player_id}"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        if data and "data" in data and len(data["data"]) > 0:
            player_data = data["data"][0]
            player_info = {
                "id": player_data.get("id"),
                "name": player_data.get("name"),
                "birth_date": player_data.get("lifeDates").get("dateOfBirth"),
                "place_of_birth": player_data.get("birthPlaceDetails").get("placeOfBirth"),
                "country_birth_id": player_data.get("birthPlaceDetails").get("countryOfBirthId"),
                "nationality_id": player_data.get("nationalityDetails").get("nationalities").get("nationalityId"),
                "height": player_data.get("attributes").get("height"),
                "position": player_data.get("attributes").get("position")}
            return player_info
        else:
            return {"id": player_id, "error": "No data"}
    else:
        print(f"Erreur lors de la récupération des données pour le joueur {player_id}: {response.status_code}")
        return {}
    
    
def _extract_information_transfert_from_JSON_api(player_id: int):
    """
    Récupère les informations de transfert d'un joueur à partir de l'API JSON de Transfermarkt.
    Args:
        player_id (str): ID du joueur.
    Returns:
        dict: Dictionnaire contenant les informations de transfert du joueur.
    """
    url = f"https://tmapi-alpha.transfermarkt.technology/transfer/history/player/{player_id}"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()

        if data.get("data", {}).get("history", {}):
            transfer_info_list = []
            history = data.get("data", {}).get("history", {})
            all_transfers = history.get("terminated", []) + history.get("pending", [])
            for transfer in all_transfers:
                details = transfer.get("details", {})
                fee_compact = details.get("fee", {}).get("compact", {})
                mv_compact = details.get("marketValue", {}).get("compact", {})
                transfer_info = {
                    "transfer_id": transfer.get("id"),
                    "player_id": details.get("playerId"),
                    "date": details.get("date"),
                    "age": details.get("age"),
                    "season": details.get("season", {}).get("display"),
                    "source_club_id": transfer.get("transferSource", {}).get("clubId"),
                    "destination_club_id": transfer.get("transferDestination", {}).get("clubId"),
                    "fee": f"{fee_compact.get('prefix', '')}{fee_compact.get('content', '')}{fee_compact.get('suffix', '')}".strip(),
                    "market_value": f"{mv_compact.get('prefix', '')}{mv_compact.get('content', '')}{mv_compact.get('suffix', '')}".strip(),
                    "type": transfer.get("typeDetails", {}).get("type")
                }
                transfer_info_list.append(transfer_info)
            return transfer_info_list
        else:
            return {"id": player_id, "error": "No data"}
    else:
        print(f"Erreur lors de la récupération des données pour le joueur {player_id}: {response.status_code}")
        return {}


###############  PLUS UTILISE CAR LENTEUR DE LA PAGE ET API MEILLEURS ###############
############# RECUPERATION DES INFORAMTIONS DEPUIS LA PAGE HTML #############


def get_player_information(player_url : str):
    
    soup = get_html_body_from_url(player_url)
    personal_informations = _extract_personal_information(soup)
    

def _extract_name_and_surname(name_soup: str):
    
    # Supprime le numéro de maillot s'il est présent
    shirt_number = name_soup.find("span", class_="data-header__shirt-number")  # type: ignore
    if shirt_number:
        shirt_number.extract()

    # Extraire le texte
    # Séparer prénom et nom
    nom_soup = name_soup.find("strong")
    nom_soup.extract()
    nom = nom_soup.get_text(strip=True)
    prenom = name_soup.text.strip()

    return {"name" :nom, "surname" : prenom}

def _extract_other_information(table_soup: str):
    colonnes_label = table_soup.select("span.info-table__content--regular")
    colonnes_info = table_soup.select("span.info-table__content--bold")
        
    
    result = {}
    
    for colonne_label, colonne_info in zip(colonnes_label,colonnes_info):
        label = colonne_label.get_text(strip=True)
        info = colonne_info.get_text(strip=True, separator=' ')
        
        if not info.strip():
            info ="No_value" 
            
        result[normalize_key(label)] = info
    
    return result

def _extract_personal_information(soup: BeautifulSoup):
    name_surname = _extract_name_and_surname(soup.find("h1", class_="data-header__headline-wrapper"))
    other_informations = _extract_other_information(soup.find("div", class_="info-table info-table--right-space"))
    
    return {**name_surname, **other_informations}

    
###### UTILITAIRES ######
    
def normalize_key(key):
    # Enlève les ":" et espaces, convertit en snake_case
    key = key.strip().lower()
    key = re.sub(r'[^a-z0-9 ]+', '', key)  # remove : and special chars
    key = key.replace(' ', '_')
    return key
    
    



