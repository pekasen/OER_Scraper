import re
import os
import shutil
import typing
import zipfile
import argparse
import requests
import pandas as pd
from glob import glob
from tqdm import tqdm
from datetime import datetime
import xml.etree.ElementTree as ET
from loguru import logger

API_URL = 'https://mediathekviewweb.de/api/query'

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

def _call_mv_api_(api_url: str, query: typing.Dict):
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
    response = requests.post(api_url, json=query, headers=headers)
    logger.debug(f"API call to {api_url} with query {query} returned status code {response.status_code}")
    if response.status_code != 200:
        return
    # Parse JSON content
    response = response.json()
    logger.debug(f"API call returned {len(response)} results")
    return response


def get_program(program: str, program_query: typing.Dict) -> typing.Optional[pd.DataFrame]:
    """
    Fetches metadata for a specified TV show from the MediathekView API.

    Args:
        program (str): The name of the TV show to retrieve data for.
        program_query (str): The query string to send to the API.

    Returns:
        DataFrame: A DataFrame containing metadata about the TV show's episodes.
    """    

    logger.debug(f"Fetching data for {program} with query: {program_query}")    

    results = _call_mv_api_(API_URL, program_query)
    if not results:
        logger.warning(f"No data return for {program}.")
        return
    
    results = (
        pd.DataFrame(
            (
                res_obj
                for res_obj in results.get("result", {"results": []}).get("results", [])
            )
        )
        .drop_duplicates("url_subtitle").reset_index(drop=True)
        .assign(permanent_id=lambda x: x.timestamp.map(lambda y: f"{program}_{y}"))
        .query("url_subtitle.notna()")
    )

    # results["PERMANENT_ID"] = df.index
    # if sendung == "zdf_heute":
    #     return df.iloc[:366,:]
    # elif sendung in ["anne_will", "caren_miosga"]:
    #     df.dropna(subset="url_subtitle", inplace=True)
    #     df = df[df["url_subtitle"] != ""].reset_index(drop=True)
    #     df["PERMANENT_ID"] = df.index
    #     return df
    # else:
    return results


def save_metadata(program, date, data, path):
    """
    Saves metadata for the specified TV show to a CSV file.

    Args:
        program (str): The name of the TV show.
        date (str): The date string to include in the file path.
        df (DataFrame): The DataFrame containing metadata to save.
        path (Path): The path to save the metadata file to.
    """
    data.to_csv(path / f"{program}_{date}.csv", index=False)

    
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
    
def save_subtitles(df, program, date):
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
        path = get_path(program, date, path)
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
    
def download_videos_as_zip(sendung, date, df, path):
    """
    Downloads videos for the specified TV show, zips them, and removes the original files.

    Args:
        sendung (str): The name of the TV show.
        date (str): The date string to include in the file path.
        df (DataFrame): The DataFrame containing video URLs to download.
    """
    base_path = get_path(sendung, date, path)
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

def parse_and_save_xml(sendung, date, path):
    """
    Parses XML subtitle files and saves them as CSV files.

    Args:
        sendung (str): The name of the TV show.
        date (str): The date string to include in the file path.

    """
    path = get_path(sendung, date,  path)
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
        df = get_program(sendung)
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
