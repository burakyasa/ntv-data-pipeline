import time
import traceback
import requests
import xml.etree.ElementTree as ET
import json
import os
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import psycopg2

load_dotenv()

LOG_FILE_NAME = os.getenv('LOG_FILE_NAME', 'ntv_fetcher.log')
OUTPUT_DIR_NAME = os.getenv('OUTPUT_DIR_NAME', 'output_ntv')
MEDIA_SUBDIR_NAME = os.getenv('MEDIA_SUBDIR_NAME', 'haber_media_ntv')
JSON_SUBDIR_NAME = os.getenv('JSON_SUBDIR_NAME', 'haber_ntv')
SLEEP_INTERVAL = int(os.getenv('SLEEP_INTERVAL', '900'))
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '5'))
TIMEZONE_OFFSET_HOURS = int(os.getenv('TIMEZONE_OFFSET_HOURS', '3'))
USER_AGENT = os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'scraper_stats')
DB_USER = os.getenv('DB_USER', 'myuser')
DB_PASS = os.getenv('DB_PASS', 'mypassword')

default_rss_urls = [
    "https://www.ntv.com.tr/son-dakika.rss", "https://www.ntv.com.tr/galeri.rss",
    "https://www.ntv.com.tr/video.rss", "https://www.ntv.com.tr/gundem.rss",
    "https://www.ntv.com.tr/turkiye.rss", "https://www.ntv.com.tr/egitim.rss",
    "https://www.ntv.com.tr/ekonomi.rss", "https://www.ntv.com.tr/ntvpara.rss",
    "https://www.ntv.com.tr/n-life.rss", "https://www.ntv.com.tr/dunya.rss",
    "https://www.ntv.com.tr/yasam.rss", "https://www.ntv.com.tr/spor.rss",
    "https://www.ntv.com.tr/sporskor.rss", "https://www.ntv.com.tr/teknoloji.rss",
    "https://www.ntv.com.tr/saglik.rss", "https://www.ntv.com.tr/sanat.rss",
    "https://www.ntv.com.tr/seyahat.rss", "https://www.ntv.com.tr/otomobil.rss"
]

RSS_URLS_STR = os.getenv('RSS_URLS', ",".join(default_rss_urls))
rss_urls = [url.strip() for url in RSS_URLS_STR.split(',') if url.strip()]

script_dir_for_log = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(script_dir_for_log, LOG_FILE_NAME)
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

def fetch_and_process_rss(session, url, directory_media):
    try:
        requests.packages.urllib3.disable_warnings()
        headers = {'User-Agent': USER_AGENT}
        
        response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT, verify=False)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        
        entries = []
        media_urls_count = 0
        successful_downloads = 0
        category_media_size_bytes = 0
        media_count_map = {}

        for entry in root.findall('.//{http://www.w3.org/2005/Atom}entry'):
            try:
                entry_id = entry.find('{http://www.w3.org/2005/Atom}id').text.split(',')[-1]
                current_time_with_offset = datetime.now() + timedelta(hours=TIMEZONE_OFFSET_HOURS)
                formatted_time = current_time_with_offset.strftime("%Y-%m-%dT%H:%M:%S")
                
                processed_entry = {
                    "id": entry_id,
                    "title": entry.find('{http://www.w3.org/2005/Atom}title').text,
                    "time": entry.find('{http://www.w3.org/2005/Atom}published').text,
                    "updated_time": entry.find('{http://www.w3.org/2005/Atom}updated').text,
                    "author": entry.find('{http://www.w3.org/2005/Atom}author').find('{http://www.w3.org/2005/Atom}name').text,
                    "url": entry.find('{http://www.w3.org/2005/Atom}link').attrib['href'],
                    "text": "",
                    "media_links": [],
                    "retrieved_time": formatted_time
                }

                content = entry.find('{http://www.w3.org/2005/Atom}content')
                if content is not None:
                    soup = BeautifulSoup(content.text, 'html.parser')
                    first_paragraph = soup.find('p')
                    if first_paragraph:
                        processed_entry["summary"] = first_paragraph.get_text()

                    all_text = soup.get_text()
                    if first_paragraph:
                        first_paragraph_text = first_paragraph.get_text()
                        processed_entry["text"] = all_text.replace(first_paragraph_text, '', 1).strip()
                    else:
                        processed_entry["text"] = all_text.strip()

                    for img in soup.find_all('img'):
                        img_url = img.get('src')
                        if img_url:
                            media_urls_count += 1
                            processed_entry["media_links"].append(img_url)
                            downloaded_size = download_media(session, img_url, entry_id, directory_media, media_count_map)
                            if downloaded_size > 0:
                                successful_downloads += 1
                                category_media_size_bytes += downloaded_size
                
                entries.append(processed_entry)

            except Exception as e:
                logging.error(f"Error processing entry: {e}\n{traceback.format_exc()}")
                pass
        
        return entries, media_urls_count, successful_downloads, category_media_size_bytes

    except requests.RequestException as e:
        logging.error(f"Error fetching RSS feed from {url}: {e}")
        return [], 0, 0, 0

def download_media(session, url, entry_id, directory_media, media_count_map):
    
    max_retries = 3
    base_network_delay = 2  

    for attempt in range(max_retries):
        try:
            headers = {'User-Agent': USER_AGENT}
            response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if response.status_code == 200:
                file_suffix = media_count_map.get(entry_id, 0)
                media_count_map[entry_id] = file_suffix + 1
                file_extension = os.path.splitext(urlparse(url).path)[1]
                if not file_extension:
                    file_extension = ".jpg"
                file_name = os.path.join(directory_media, f'{entry_id}_{file_suffix}{file_extension}')
                
                with open(file_name, 'wb') as f:
                    f.write(response.content)
                
                return os.path.getsize(file_name)

            elif response.status_code == 429:

                rate_limit_delay = 2 
                logging.warning(f"Rate limit hit for {url}. Retrying in {rate_limit_delay} seconds...")
                time.sleep(rate_limit_delay)

                continue

            else:

                logging.warning(f"Failed to download media from {url}, status code: {response.status_code}")
                return 0

        except requests.RequestException as e:

            delay = base_network_delay * (2 ** attempt)
            logging.error(f"Network error on attempt {attempt + 1} for {url}: {e}. Retrying in {delay} seconds...")
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                logging.error(f"All network retries failed for {url}.")
                return 0 

    logging.warning(f"All retries failed for {url} after {max_retries} attempts.")
    return 0


def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to PostgreSQL database: {e}")
        return None

def create_stats_table():
    conn = get_db_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tour_stats (
                    id SERIAL PRIMARY KEY,
                    tour_end_time TIMESTAMPTZ DEFAULT NOW(),
                    duration_seconds FLOAT,
                    json_size_mb FLOAT,
                    media_size_mb FLOAT,
                    total_media_urls INTEGER,
                    successful_downloads INTEGER,
                    overall_success_rate FLOAT
                );
            """)
            conn.commit()
            logging.info("'tour_stats' table exists or was successfully created.")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()


def log_stats_to_postgres(duration, json_size, media_size, total_urls, successful_downloads, success_rate):
    conn = get_db_connection()
    if conn is None:
        return
    
    sql = """
        INSERT INTO tour_stats (duration_seconds, json_size_mb, media_size_mb, total_media_urls, successful_downloads, overall_success_rate)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (duration, json_size, media_size, total_urls, successful_downloads, success_rate))
            conn.commit()
            logging.info("Successfully inserted tour stats into PostgreSQL.")
    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()

def main():
    create_stats_table()
    
    session = requests.Session()
    
    script_directory = os.path.dirname(os.path.abspath(__file__))
    output_directory = os.path.join(script_directory, OUTPUT_DIR_NAME) 
    
    while True:
        tour_start_time = time.time()
        tour_total_json_size_bytes = 0
        tour_total_media_size_bytes = 0
        tour_total_media_urls = 0
        tour_total_successful_downloads = 0
        tour_stats = {}

        for rss_url in rss_urls:
            current_time_with_offset = datetime.now() + timedelta(hours=TIMEZONE_OFFSET_HOURS)
            current_time_str = current_time_with_offset.strftime("%Y-%m-%d_%H-%M-%S")
            
            url_parts = rss_url.split('/')
            category_name = url_parts[3].replace('.rss', '') if len(url_parts) > 3 else "unknown"
            print(f"Processing category: {category_name}...")
            
            directory_media = os.path.join(output_directory, MEDIA_SUBDIR_NAME, f'{current_time_str}_{category_name}')
            os.makedirs(directory_media, exist_ok=True)
            
            directory_json = os.path.join(output_directory, JSON_SUBDIR_NAME)
            os.makedirs(directory_json, exist_ok=True)

            entries, media_urls, successful_downloads, category_media_size = fetch_and_process_rss(session, rss_url, directory_media)
            
            tour_total_media_size_bytes += category_media_size
            tour_total_media_urls += media_urls
            tour_total_successful_downloads += successful_downloads
            
            json_file_name = f"data_ntv_{current_time_str}_{category_name}.json"
            json_file_path = os.path.join(directory_json, json_file_name)
            
            with open(json_file_path, mode='w', encoding='utf-8') as f:
                json.dump(entries, f, indent=4, ensure_ascii=False)
            
            if os.path.exists(json_file_path):
                tour_total_json_size_bytes += os.path.getsize(json_file_path)

            success_percentage = (successful_downloads / media_urls * 100) if media_urls > 0 else 0
            
            tour_stats[category_name] = {
                "entries_fetched": len(entries),
                "media_urls_found": media_urls,
                "downloaded_successfully": successful_downloads,
                "success_percentage": success_percentage
            }

        tour_end_time = time.time()
        tour_duration = tour_end_time - tour_start_time
        tour_total_json_size_mb = tour_total_json_size_bytes / (1024 * 1024)
        tour_total_media_size_mb = tour_total_media_size_bytes / (1024 * 1024)
        overall_success_rate = (tour_total_successful_downloads / tour_total_media_urls * 100) if tour_total_media_urls > 0 else 0
        
        log_stats_to_postgres(
            tour_duration,
            tour_total_json_size_mb,
            tour_total_media_size_mb,
            tour_total_media_urls,
            tour_total_successful_downloads,
            overall_success_rate
        )

        logging.info("========== TOUR COMPLETED ==========")
        for category, stats in tour_stats.items():
            logging.info(f"Category: {category} >> Fetched: {stats['entries_fetched']}, Media URLs: {stats['media_urls_found']}, Successful Downloads: {stats['downloaded_successfully']}, Success Rate: {stats['success_percentage']:.2f}%")
        
        logging.info(f"Tour Duration: {tour_duration:.2f} seconds")
        logging.info(f"Total JSON Size: {tour_total_json_size_mb:.4f} MB")
        logging.info(f"Total Media Size: {tour_total_media_size_mb:.2f} MB")
        logging.info(f"Overall Success Rate: {overall_success_rate:.2f}%")
        logging.info("======================================")

        print("\n========== TOUR COMPLETED ==========")
        print(f"TOUR DURATION: {tour_duration:.2f} sec ")
        print("======================================\n")

        time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    main()