"""Contains functions for scraping metadata"""

# pylint: disable=R1702,W0622

import os
import pathlib
import typing
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime

import pandas as pd
import requests
from loguru import logger
from tqdm import tqdm

API_URL = "https://mediathekviewweb.de/api/query"
XML_FOLDER = "xml-subtitles/"
VIDEO_FOLDER = "videos/"
SUBTITLES_FOLDER = "subtitles/"
TIMEOUT = 10


def _call_mv_api_(api_url: str, query: typing.Dict):
    """
    Calls the MediathekView API with a given query.

    Args:
        api_url (str): The API endpoint URL.
        query (str): The query string to send to the API.

    Returns:
        dict: The JSON response from the API, or None if the call fails.
    """
    headers = {"User-Agent": "ax mvclient 0.1.1", "Content-Type": "text/plain"}
    response = requests.post(api_url, json=query, headers=headers, timeout=TIMEOUT)
    logger.debug(
        f"API call to {api_url} with query {query} returned status code {response.status_code}"
    )
    if response.status_code != 200:
        return None
    # Parse JSON content
    response = response.json()
    logger.debug(f"API call returned {len(response.get("result"))} results")
    return response


def get_program(
    program: str, program_query: typing.Dict
) -> typing.Optional[pd.DataFrame]:
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
        return None

    results = (
        pd.DataFrame(
            (
                res_obj
                for res_obj in results.get("result", {"results": []}).get("results", [])
            )
        )
        .drop_duplicates("url_subtitle")
        .reset_index(drop=True)
        .assign(
            permanent_id=lambda x: x.timestamp.map(lambda y: f"{program}_{y}"),
            timestamp=lambda x: pd.to_datetime(x.timestamp, unit="s"),
        )
        .query("url_subtitle.str.startswith('https')")
    )

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
    path = get_path(program, date, path)
    data.to_csv(path / f"{program}_{date}.csv", index=False)


def zip_folder(folder_path, zip_path):
    """
    Zips the contents of a folder into a ZIP file.

    Args:
        folder_path (str): The path of the folder to zip.
        zip_path (str): The path of the resulting ZIP file.
    """
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in tqdm(
            os.walk(folder_path), desc="Zip Videos", leave=False
        ):
            for file in files:
                zipf.write(
                    os.path.join(root, file),
                    os.path.relpath(os.path.join(root, file), folder_path),
                )


def get_path(program: str, date: datetime, path: pathlib.Path) -> pathlib.Path:
    """
    Creates a path for the specified TV show and date.

    Args:
        program (str): The name of the TV show.
        date (str): The date string to include in the file path.

    Returns:
        Path: The path to save the TV show data to.
    """
    out_path = path / f"{program}/{date}/"
    if not out_path.exists():
        out_path.mkdir(parents=True)
    return out_path


def save_subtitles(df, program, date, output_path):
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
        response = requests.get(url, timeout=TIMEOUT)
        if response.status_code == 200:
            xml_data = response.content
            xml_file_path = path / f"{id}.xml"
            with xml_file_path.open("wb") as f:
                f.write(xml_data)
        return xml_file_path

    output_path = get_path(program, date, output_path)
    xml_path = output_path / XML_FOLDER
    xml_paths = []
    if not xml_path.exists():
        xml_path.mkdir(parents=True)
    for _, row in tqdm(df.iterrows(), total=len(df), leave=False):
        # if isinstance(row["url_subtitle"], str) and  row["url_subtitle"].startswith("https://"):
        xml_paths.append(
            get_subtitles(row["url_subtitle"], row["permanent_id"], xml_path)
        )
    df["xml_path"] = xml_paths
    return df


def download_video(file_name, url):
    """
    Downloads a video from a URL and saves it to a file.

    Args:
        file_name (str): The name of the file to save the video to.
        url (str): The URL of the video to download.

    Returns:
        bool: True if the download was successful, False otherwise.
    """
    response = requests.get(url, stream=True, timeout=TIMEOUT)
    if response.status_code == 200:
        with open(file_name, "wb") as f:
            for chunk in tqdm(
                response.iter_content(chunk_size=1024),
                desc="Downloading",
                unit="KB",
                unit_scale=True,
                leave=False,
                position=2,
            ):
                if chunk:
                    f.write(chunk)
                    f.flush()
        return True
    return False


def download_videos_as_zip(sendung, date, df, path, zipped=False):
    """
    Downloads videos for the specified TV show, zips them, and removes the original files.

    Args:
        sendung (str): The name of the TV show.
        date (str): The date string to include in the file path.
        df (DataFrame): The DataFrame containing video URLs to download.
    """
    base_path = get_path(sendung, date, path)
    video_path = base_path / VIDEO_FOLDER
    if not video_path.exists():
        video_path.mkdir(parents=True)

    # temp_folder = tempfile.mkdtemp()

    for _, row in tqdm(df.iterrows(), total=len(df), position=1, leave=False):
        download_video(video_path / f'{row["permanent_id"]}.mp4', row["url_video_low"])
    if zipped:
        zip_folder(path, path + ".zip")


def xml_to_df(xml_path: pathlib.Path) -> pd.DataFrame:
    """
    Parses an XML file containing subtitles into a DataFrame.

    Args:
        xml_path (str): The path of the XML file to parse.

    Returns:
        DataFrame: A DataFrame containing the parsed subtitle data.
    """
    ns = {"tt": "http://www.w3.org/ns/ttml"}
    tree = ET.parse(xml_path)
    root = tree.getroot()
    dict_list = []
    for p in root.findall(".//tt:p", ns):
        # xml_id = p.attrib["{http://www.w3.org/XML/1998/namespace}id"]
        begin = p.attrib["begin"]
        end = p.attrib["end"]
        text = ""
        color = ""
        for span in p.findall(".//tt:span", ns):
            if isinstance(span.text, str):
                text += " " + span.text
                if "style" in span.attrib:
                    style_id = span.attrib["style"]
                    for style in root.findall(".//tt:style", ns):
                        if (
                            style.attrib["{http://www.w3.org/XML/1998/namespace}id"]
                            == style_id
                        ):
                            color = style.attrib.get(
                                "{http://www.w3.org/XML/1998/namespace}id", ""
                            )
                            break
        dict_list.append(
            {"text": text, "color": color, "start_time": begin, "end_time": end}
        )

    return pd.DataFrame(dict_list)


def parse_xml_df(xml_df):
    """
    Parses a DataFrame containing subtitle data into a more structured format.

    Args:
        xml_df (DataFrame): The DataFrame containing subtitle data.

    Returns:
        DataFrame: A DataFrame containing the parsed subtitle data.
    """
    dict_list = []
    text = ""
    color = ""
    start_time = ""
    end_time = ""
    for _, row in xml_df.iterrows():
        if "* Gong *" in row["text"]:
            continue
        new_text = row["text"]
        new_color = row["color"]
        new_start_time = row["start_time"]
        new_end_time = row["end_time"]
        if new_color != color:
            dict_list.append(
                {
                    "text": text.strip(),
                    "color": color,
                    "start_time": start_time,
                    "end_time": end_time,
                }
            )
            text = new_text
            color = new_color
            start_time = new_start_time
            end_time = new_end_time
        elif (
            new_text.strip().endswith(".")
            or new_text.strip().endswith("?")
            or new_text.strip().endswith("!")
        ):
            text += " " + new_text
            end_time = new_end_time
            dict_list.append(
                {
                    "text": text.strip(),
                    "color": color,
                    "start_time": start_time,
                    "end_time": end_time,
                }
            )
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


def parse_and_save_xml(program, date, path, data):
    """
    Parses XML subtitle files and saves them as CSV files.

    Args:
        sendung (str): The name of the TV show.
        date (str): The date string to include in the file path.

    """
    program_path = get_path(program, date, path)
    xml_folder = program_path / XML_FOLDER
    parsed_xml_folder = program_path / SUBTITLES_FOLDER
    if not parsed_xml_folder.exists():
        parsed_xml_folder.mkdir(parents=True)

    input_path_list = data.xml_path.map(lambda x: xml_folder / x).tolist()
    output_path_list = data.permanent_id.map(
        lambda x: parsed_xml_folder / f"{x}.csv"
    ).tolist()

    logger.info(f"Parsing {len(input_path_list)} XML files for {program} on {date}")

    for xml_path, csv_path in tqdm(
        zip(input_path_list, output_path_list), desc="Parse XML", leave=False
    ):
        df = xml_to_df(xml_path)
        df = parse_xml_df(df)
        df.to_csv(
            csv_path,
            index=False,
        )
