import re
import os
import shutil
import zipfile
import argparse
import requests
import pandas as pd
from glob import glob
from tqdm import tqdm
from datetime import datetime
import xml.etree.ElementTree as ET

from oer_scraper.cli import BASE_PATH

DATE = datetime.today().strftime("%d%m%Y")
nachrichtensendungen = ["markus_lanz", "maybrit_illner", "maischberger", "hart_aber_fair","tagesschau","tagesthemen", "zdf_heute", "heute_journal", "anne_will", "caren_miosga"]

def create_base_folder():
    """
    Creates a folder named "Broadcast_Subtitles" at the same level as the script if it does not already exist.

    This function determines the directory where the script is located and checks if the folder "Broadcast_Subtitles" exists.
    If the folder does not exist, it creates the folder at the same level as the script.

    Returns:
        None
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_folder_path = os.path.join(script_dir, "Broadcast_Subtitles")
    if not os.path.exists(base_folder_path):
        os.makedirs(base_folder_path)

def get_sendung(sendung:str):
    """
    Fetches metadata for a specified TV show from the MediathekView API.

    Args:
        sendung (str): The name of the TV show to retrieve data for.

    Returns:
        DataFrame: A DataFrame containing metadata about the TV show's episodes.
    """
    def call_mv_api(api_url, query):
        """
        Calls the MediathekView API with a given query.

        Args:
            api_url (str): The API endpoint URL.
            query (str): The query string to send to the API.

        Returns:
            dict: The JSON response from the API, or None if the call fails.
        """
        headers = {
            'User-Agent': 'ax mvclient 0.1.1',
            'Content-Type': 'text/plain'
        }
        response = requests.post(api_url, data=query, headers=headers)
        return response.json() if response.status_code == 200 else None
    
    def conditions(entry, sendung):
        """
        Checks if an entry matches the conditions for the specified TV show.

        Args:
            entry (dict): The entry to check.
            sendung (str): The name of the TV show.

        Returns:
            dict or None: The entry if it matches the conditions, otherwise None.
        """
        if sendung == "heute_journal":
            if "heute journal" in entry['topic'].lower() and entry['url_subtitle'] != "":
                return entry
            else:
                return None
        elif sendung == "zdf_heute":
            if entry['url_subtitle'] != "" and entry["duration"] > 500:
                return entry
            else:
                return None
        elif sendung == "tagesschau":
            if "tagesschau 20:00 Uhr" in entry['title'] and entry['url_subtitle'] != "":
                return entry
            else:
                return None
        elif sendung == "tagesthemen":
            pattern = r"tagesthemen \d{2}:\d{2} Uhr, \d{2}\.\d{2}\.\d{4}"
            if "tagesthemen" in entry["topic"].lower() and entry['url_subtitle'] != "" and re.match(pattern, entry['title']) is not None:
                return entry
            else:
                return None
        elif sendung == "anne_will":
            if 'Anne Will' in entry["topic"]:
                return entry
            else:
                return None
        elif sendung == "caren_miosga":
            if 'Caren Miosga' in entry["topic"]:
                return entry
            else:
                return None
        elif sendung == "hart_aber_fair":
            if 'Hart aber fair' in entry["topic"]:
                return entry
            else:
                return None
        elif sendung == "maischberger":
            if 'maischberger' in entry["topic"] and "maischberger am" in entry["title"]:
                return entry
            else:
                return None
        elif sendung == "markus_lanz":
            if 'Markus Lanz' in entry["topic"]:
                return entry
            else:
                return None
        elif sendung == "maybrit_illner":
            if 'maybrit illner' in entry["topic"]:
                return entry
            else:
                return None
        else:
            raise KeyError(f"{sendung} does not exist!")
        
    api_url = 'https://mediathekviewweb.de/api/query'
    sendungs_dict = {
        "tagesschau": '{"queries":[{"fields":["title","topic"],"query":"tagesschau"},' \
            '{"fields":["channel"],"query":"ard"}],"sortBy":"timestamp","sortOrder":"desc",' \
            '"future":"false","offset":"0","size":"8000"}',
        "tagesthemen": '{"queries":[{"fields":["title","topic"],"query":"tagesthemen"},' \
            '{"fields":["channel"],"query":"ard"}],"sortBy":"timestamp","sortOrder":"desc",' \
            '"future":"false","offset":"0","size":"8000"}',
        "zdf_heute": '{"queries":[{"fields":["title","topic"],"query":"heute 19:00 uhr"},' \
            '{"fields":["channel"],"query":"zdf"}],"sortBy":"timestamp","sortOrder":"desc",' \
            '"future":"false","offset":"0","size":"8000"}',
        "heute_journal":'{"queries":[{"fields":["title","topic"],"query":"heute journal"},' \
            '{"fields":["channel"],"query":"zdf"}],"sortBy":"timestamp","sortOrder":"desc",' \
            '"future":"false","offset":"0","size":"8000"}',
        "hart_aber_fair":'{"queries":[{"fields":["title","topic"],"query":"hart aber fair"},' \
            '{"fields":["channel"],"query":"ard"}],"sortBy":"timestamp","sortOrder":"desc",' \
            '"future":"false","offset":"0","size":"8000"}',
        "anne_will":'{"queries":[{"fields":["title","topic"],"query":"anne will"},' \
            '{"fields":["channel"],"query":"ard"}],"sortBy":"timestamp","sortOrder":"desc",' \
            '"future":"false","offset":"0","size":"8000"}',
        "caren_miosga":'{"queries":[{"fields":["title","topic"],"query":"caren miosga"},' \
            '{"fields":["channel"],"query":"ard"}],"sortBy":"timestamp","sortOrder":"desc",' \
            '"future":"false","offset":"0","size":"8000"}',
        "maischberger":'{"queries":[{"fields":["title","topic"],"query":"maischberger"},' \
            '{"fields":["channel"],"query":"ard"}],"sortBy":"timestamp","sortOrder":"desc",' \
            '"future":"false","offset":"0","size":"8000"}',
        "maybrit_illner":'{"queries":[{"fields":["title","topic"],"query":"maybrit illner"},' \
            '{"fields":["channel"],"query":"zdf"}],"sortBy":"timestamp","sortOrder":"desc",' \
            '"future":"false","offset":"0","size":"8000"}',
        "markus_lanz":'{"queries":[{"fields":["title","topic"],"query":"Markus Lanz"},' \
            '{"fields":["channel"],"query":"zdf"}],"sortBy":"timestamp","sortOrder":"desc",' \
            '"future":"false","offset":"0","size":"8000"}',
        }
    
    dct_list = list()
    result = call_mv_api(api_url, sendungs_dict[sendung])
    if result:
        for idx, entry in enumerate(result['result']['results']):
            if conditions(entry, sendung) is not None:
                dct_list.append(entry)
            else:
                continue
    else:
        print("Failed to retrieve data from the API.")
    df = pd.DataFrame(dct_list).drop_duplicates("url_subtitle").reset_index(drop=True)
    df["PERMANENT_ID"] = df.index
    if sendung == "zdf_heute":
        return df.iloc[:366,:]
    elif sendung in ["anne_will", "caren_miosga"]:
        df.dropna(subset="url_subtitle", inplace=True)
        df = df[df["url_subtitle"] != ""].reset_index(drop=True)
        df["PERMANENT_ID"] = df.index
        return df
    else:
        return df
    
def get_path(sendung, date):
    """
    Constructs the file path for storing data for the specified TV show and date.

    Args:
        sendung (str): The name of the TV show.
        date (str): The date string to include in the path.

    Returns:
        str: The constructed file path.
    """
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
    if sendung == "tagesschau":
        path = os.path.join(BASE_PATH, "ARD_Tagesschau/")
    elif sendung == "tagesthemen":
        path = os.path.join(BASE_PATH, "ARD_Tagesthemen/")
    elif sendung == "zdf_heute":
        path = os.path.join(BASE_PATH, "ZDF_Heute/")
    elif sendung == "heute_journal":
        path = os.path.join(BASE_PATH, "ZDF_Heute_Journal/")
    elif sendung == "anne_will":
        path = os.path.join(BASE_PATH, "Anne_Will/")
    elif sendung == "caren_miosga":
        path = os.path.join(BASE_PATH, "Caren_Miosga/")
    elif sendung == "hart_aber_fair":
        path = os.path.join(BASE_PATH, "Hart_aber_Fair/")
    elif sendung == "maischberger":
        path = os.path.join(BASE_PATH, "Maischberger/")
    elif sendung == "maybrit_illner":
        path = os.path.join(BASE_PATH, "Maybrit_Illner/")
    elif sendung == "markus_lanz":
        path = os.path.join(BASE_PATH, "Markus_Lanz/")
    else:
        raise KeyError(f"{sendung} does not exist!")
    if not os.path.exists(path):
        os.makedirs(path)
    path += f"{date}/"
    return path

def save_metadata(sendung, date, df):
    """
    Saves metadata for the specified TV show to a CSV file.

    Args:
        sendung (str): The name of the TV show.
        date (str): The date string to include in the file path.
        df (DataFrame): The DataFrame containing metadata to save.
    """
    path = get_path(sendung, date)
    if not os.path.exists(path):
        os.makedirs(path)
    path += "metadata.csv"
    df.to_csv(path, index=False)
    
def zip_folder(folder_path, zip_path):
    """
    Zips the contents of a folder into a ZIP file.

    Args:
        folder_path (str): The path of the folder to zip.
        zip_path (str): The path of the resulting ZIP file.
    """
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in tqdm(os.walk(folder_path), desc="Zip Videos", leave=False):
            for file in files:
                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), folder_path))
    
def save_subtitles(df, sendung, date):
    """
    Saves subtitles for each episode of the specified TV show.

    Args:
        df (DataFrame): The DataFrame containing episode data.
        sendung (str): The name of the TV show.
        date (str): The date string to include in the file path.
    """
    def get_subtitles(url, id, path):
        """
        Fetches and saves subtitles from a URL.

        Args:
            url (str): The URL of the subtitles.
            id (int): The ID to use for the saved subtitle file.
            path (str): The path to save the subtitle file to.
        """
        response = requests.get(url)
        if response.status_code == 200:
            xml_data = response.content
            with open(f"{path}{id}.xml", "wb") as f:
                f.write(xml_data)
    path = get_path(sendung, date)
    path += "XML_Subtitles/"
    if not os.path.exists(path):
        os.makedirs(path)
    for idx, row in tqdm(df.iterrows(), total = len(df), leave = False):
        if isinstance(row["url_subtitle"], str):
            if row["url_subtitle"].startswith("https://"):
                get_subtitles(row["url_subtitle"], row["PERMANENT_ID"], path)
                
def download_video(file_name, url):
    """
    Downloads a video from a URL and saves it to a file.

    Args:
        file_name (str): The name of the file to save the video to.
        url (str): The URL of the video to download.

    Returns:
        bool: True if the download was successful, False otherwise.
    """
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(file_name, "wb") as f:
            for chunk in tqdm(response.iter_content(chunk_size=1024), desc="Downloading", unit="KB", unit_scale=True, leave=False, position = 2):
                if chunk:  
                    f.write(chunk)
                    f.flush() 
        return True
    else:
        return False
    
def download_videos_as_zip(sendung, date, df):
    """
    Downloads videos for the specified TV show, zips them, and removes the original files.

    Args:
        sendung (str): The name of the TV show.
        date (str): The date string to include in the file path.
        df (DataFrame): The DataFrame containing video URLs to download.
    """
    base_path = get_path(sendung, date)
    path = base_path + "Videos"
    if not os.path.exists(path):
        os.makedirs(path)
    for idx, row in tqdm(df.iterrows(), total = len(df), position = 1, leave = False):
        download_video(f'{path}/{row["PERMANENT_ID"]}.mp4', row["url_video_low"])
    zip_folder(path, path+".zip")
    shutil.rmtree(path)
    
def xml_to_df(xml_path:str):
    """
    Parses an XML file containing subtitles into a DataFrame.

    Args:
        xml_path (str): The path of the XML file to parse.

    Returns:
        DataFrame: A DataFrame containing the parsed subtitle data.
    """
    ns = {'tt': 'http://www.w3.org/ns/ttml'}
    tree = ET.parse(xml_path)
    root = tree.getroot()
    dict_list = []
    for p in root.findall('.//tt:p', ns):
        xml_id = p.attrib['{http://www.w3.org/XML/1998/namespace}id']
        begin = p.attrib['begin']
        end = p.attrib['end']
        text = ''
        color = ''
        for span in p.findall('.//tt:span', ns):
            if isinstance(span.text, str):
                text += " " + span.text
                if 'style' in span.attrib:
                    style_id = span.attrib['style']
                    for style in root.findall('.//tt:style', ns):
                        if style.attrib['{http://www.w3.org/XML/1998/namespace}id'] == style_id:
                            color = style.attrib.get('{http://www.w3.org/XML/1998/namespace}id', '')
                            break
        dict_list.append({'text': text, 'color': color, 'start_time': begin, 'end_time': end})
    return pd.DataFrame(dict_list)

def parse_xml_df(xml_df):
    """
    Parses a DataFrame containing subtitle data into a more structured format.

    Args:
        xml_df (DataFrame): The DataFrame containing subtitle data.

    Returns:
        DataFrame: A DataFrame containing the parsed subtitle data.
    """
    dict_list = list()
    text = ""
    color = ""
    start_time = ""
    end_time = ""
    for idx, row in xml_df.iterrows():
        if "* Gong *" in row["text"]:
            continue
        new_text = row["text"]
        new_color = row["color"]
        new_start_time = row["start_time"]
        new_end_time = row["end_time"]
        if new_color != color:
            dict_list.append(dict(
                text = text.strip(),
                color = color,
                start_time = start_time,
                end_time = end_time
                ))
            text = new_text
            color = new_color
            start_time = new_start_time
            end_time = new_end_time
        elif new_text.strip().endswith(".") or new_text.strip().endswith("?") or new_text.strip().endswith("!"):
            text += " " + new_text
            end_time = new_end_time
            dict_list.append(dict(
                text = text.strip(),
                color = color,
                start_time = start_time,
                end_time = end_time
                ))
            text = ""
            color = ""
            start_time = ""
            end_time = ""
        else:
            text += " " + new_text
            end_time = new_end_time

    parsed_df = pd.DataFrame(dict_list)
    parsed_df = parsed_df[parsed_df["text"] != ""]
    parsed_df["text"] = parsed_df["text"].apply(lambda x: x.replace("  ", " "))
    return parsed_df

def parse_and_save_xml(sendung, date):
    """
    Parses XML subtitle files and saves them as CSV files.

    Args:
        sendung (str): The name of the TV show.
        date (str): The date string to include in the file path.
    """
    path = get_path(sendung, date)
    xml_folder = path + "XML_Subtitles/"
    parsed_xml_folder = path + "Subtitles/"
    if not os.path.exists(parsed_xml_folder):
        os.makedirs(parsed_xml_folder)
    path_list = glob(os.path.join(xml_folder, "*.xml"))
    for xml_path in tqdm(path_list, total=len(path_list), desc="Parse XML", leave=False):
        df = xml_to_df(xml_path)
        df = parse_xml_df(df)
        df.to_csv(xml_path.replace("XML_Subtitles", "Subtitles").replace("xml", "csv"), index=False)
    
    
def main(subtitles:bool=True, parsed:bool=True, videos:bool=True):
    """
    Main function to download and process TV show data including metadata, subtitles, and videos.

    Args:
        subtitles (bool): If True, download subtitles.
        parsed (bool): If True, parse subtitles.
        videos (bool): If True, download videos.
    """
    create_base_folder()
    for sendung in tqdm(nachrichtensendungen, desc="Scrape News", total = len(nachrichtensendungen)):
        df = get_sendung(sendung)
        save_metadata(sendung, DATE, df)
        if subtitles:
            save_subtitles(df, sendung, DATE)
        if parsed:
            parse_and_save_xml(sendung, DATE)
        if videos:
            download_videos_as_zip(sendung, DATE, df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and process TV show data.")
    parser.add_argument('--subtitles', type=int, default=1, help='Download subtitles (1 for True, 0 for False)')
    parser.add_argument('--parsed', type=int, default=1, help='Parse subtitles (1 for True, 0 for False)')
    parser.add_argument('--videos', type=int, default=0, help='Download videos (1 for True, 0 for False)')
    args = parser.parse_args()

    main(subtitles=bool(args.subtitles), parsed=bool(args.parsed), videos=bool(args.videos))
