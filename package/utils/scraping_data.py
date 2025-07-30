import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import json
import random
from multiprocessing import current_process

LEAGUE_URLS = [
    {"name": "Ligue 1", "url": "https://www.transfermarkt.com/ligue-1/startseite/wettbewerb/FR1"},
    {"name": "Serie A", "url": "https://www.transfermarkt.com/serie-a/startseite/wettbewerb/IT1"}, 
    {"name": "Bundesliga", "url": "https://www.transfermarkt.com/bundesliga/startseite/wettbewerb/L1"},
    {"name": "Premier League", "url": "https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1"},
    {"name": "La Liga", "url": "https://www.transfermarkt.com/laliga/startseite/wettbewerb/ES1"},
    {"name": "Eredivisie", "url": "https://www.transfermarkt.com/eredivisie/startseite/wettbewerb/NL1"},
    {"name": "Primeira Liga", "url": "https://www.transfermarkt.com/primeira-liga/startseite/wettbewerb/PT1"}


    
    # {"name": "Ligue 2", "url": "https://www.transfermarkt.com/ligue-2/startseite/wettbewerb/FR2"}
    # {"name": "Serie B", "url": "https://www.transfermarkt.com/serie-b/startseite/wettbewerb/IT2"},
    # {"name": "2. Bundesliga", "url": "https://www.transfermarkt.com/2-bundesliga/startseite/wettbewerb/L2"},
    # {"name": "Championship", "url": "https://www.transfermarkt.com/championship/startseite/wettbewerb/GB2"},
    # {"name": "La Liga 2", "url": "https://www.transfermarkt.com/laliga2/startseite/wettbewerb/ES2"},
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
    # Headers plus variés et réalistes
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
    ]
    
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "max-age=0",
        "dnt": "1",
        "referer": "https://www.transfermarkt.com/",
        "sec-ch-ua": "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": random.choice(user_agents)
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    # Ajouter un délai aléatoire avant chaque requête
    delay = random.uniform(2, 5)  # Entre 2 et 5 secondes
    time.sleep(delay)
    
    for attempt in range(6):
        try:
            response = session.get(url, timeout=15)
            if response.status_code == 200:
                break
            elif response.status_code == 429:
                print("Limite de taux dépassée (429). Attente plus longue...")
                time.sleep(random.uniform(30, 60))
            elif response.status_code == 503:
                print("503 reçu. Le serveur est indisponible.")
                time.sleep(random.uniform(20, 40))
            else:
                print(f"Erreur HTTP : {response.status_code}")
        except requests.RequestException as e:
            print(f"Erreur de connexion pour {url}: {e}")
            
        if attempt < 5:
            wait_time = random.uniform(45, 90)  # Attente plus longue et aléatoire
            print(f"Nouvelle tentative dans {wait_time:.1f}s...")
            time.sleep(wait_time)

    return BeautifulSoup(response.text, "html.parser")
    
    
def get_club_urls(url: str,year: int = None):
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
    if not club_table_summary:
        print("Table des clubs non trouvée")
        return pd.DataFrame()
        
    club_rows = club_table_summary.select("table.items tbody tr")

    clubs = []
    for row in club_rows:
        link = row.select_one("td.hauptlink a")
        if link:
            club_id = link["href"].split("/")[-3]
            club_name = link.text.strip()
            if year:
                club_url = "https://www.transfermarkt.com" + "/".join(link["href"].split("/")[0:5]) + f"/plus/?saison_id={year}"
            else:
                club_url = "https://www.transfermarkt.com" + "/".join(link["href"].split("/")[0:5])
            clubs.append({"id": club_id, "name": club_name, "url": club_url})

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
    if not player_table_summary:
        print("Table des joueurs non trouvée")
        return pd.DataFrame()
        
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
    
    # Délai aléatoire entre les appels API
    delay = random.uniform(1, 3)
    time.sleep(delay)
    
    try:
        player_info = _extract_information_player_from_JSON_api(player_id)
        transfer_info = _extract_information_transfert_from_JSON_api(player_id)
        
        # Écriture ligne par ligne pour les informations du joueur
        output_file = os.path.join(OUTPUT_DIR, f"players_part_{process_id}.jsonl")
        with open(output_file, "a", encoding="utf-8") as f:
            json.dump(player_info, f, ensure_ascii=False)
            f.write("\n")
            
        # Écriture ligne par ligne pour les transferts
        output_file = os.path.join(OUTPUT_DIR, f"transfert_part_{process_id}.jsonl")
        with open(output_file, "a", encoding="utf-8") as f:
            json.dump(transfer_info, f, ensure_ascii=False)
            f.write("\n")
            
    except Exception as e:
        print(f"Erreur pour le joueur {player_id}: {e}")
        # Sauvegarder l'erreur pour ne pas perdre l'information
        error_info = {"player_id": player_id, "error": str(e)}
        output_file = os.path.join(OUTPUT_DIR, f"errors_part_{process_id}.jsonl")
        with open(output_file, "a", encoding="utf-8") as f:
            json.dump(error_info, f, ensure_ascii=False)
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
    
    headers = {
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15"
        ]),
        "Accept": "application/json",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
    }
    
    for attempt in range(3):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data and "data" in data and len(data["data"]) > 0:
                    player_data = data["data"][0]
                    player_info = {
                        "id": player_data.get("id"),
                        "name": player_data.get("name"),
                        "birth_date": player_data.get("lifeDates", {}).get("dateOfBirth"),
                        "place_of_birth": player_data.get("birthPlaceDetails", {}).get("placeOfBirth"),
                        "country_birth_id": player_data.get("birthPlaceDetails", {}).get("countryOfBirthId"),
                        "nationality_id": player_data.get("nationalityDetails", {}).get("nationalities", {}).get("nationalityId"),
                        "height": player_data.get("attributes", {}).get("height"),
                        "position": player_data.get("attributes", {}).get("position")
                    }
                    return player_info
                else:
                    return {"id": player_id, "error": "No data"}
            elif response.status_code == 429:
                print(f"Rate limit pour joueur {player_id}, attente...")
                time.sleep(random.uniform(5, 15))
            else:
                print(f"Erreur HTTP {response.status_code} pour joueur {player_id}")
                
        except Exception as e:
            print(f"Erreur requête pour joueur {player_id}: {e}")
            
        if attempt < 2:
            time.sleep(random.uniform(2, 5))
    
    return {"id": player_id, "error": "Failed after retries"}
    
    
def _extract_information_transfert_from_JSON_api(player_id: int):
    """
    Récupère les informations de transfert d'un joueur à partir de l'API JSON de Transfermarkt.
    Args:
        player_id (str): ID du joueur.
    Returns:
        dict: Dictionnaire contenant les informations de transfert du joueur.
    """
    url = f"https://tmapi-alpha.transfermarkt.technology/transfer/history/player/{player_id}"
    
    headers = {
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15"
        ]),
        "Accept": "application/json",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
    }
    
    for attempt in range(3):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
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
                    return {"id": player_id, "error": "No transfer data"}
            elif response.status_code == 429:
                print(f"Rate limit pour transferts joueur {player_id}, attente...")
                time.sleep(random.uniform(5, 15))
            else:
                print(f"Erreur HTTP {response.status_code} pour transferts joueur {player_id}")
                
        except Exception as e:
            print(f"Erreur requête transferts pour joueur {player_id}: {e}")
            
        if attempt < 2:
            time.sleep(random.uniform(2, 5))
    
    return {"id": player_id, "error": "Failed after retries"}


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
    
def normalize_key(key) :
    # Enlève les ":" et espaces, convertit en snake_case
    key = key.strip().lower()
    key = re.sub(r'[^a-z0-9 ]+', '', key)  # remove : and special chars
    key = key.replace(' ', '_')
    return key