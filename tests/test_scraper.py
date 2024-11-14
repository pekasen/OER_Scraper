import pytest
import pandas as pd
import pathlib
import requests
from unittest.mock import patch, MagicMock

from oer_scraper.scraper import (
    _call_mv_api_,
    get_program,
    save_metadata,
    zip_folder,
    get_path,
    save_subtitles,
    download_video,
    download_videos_as_zip,
    xml_to_df,
    parse_xml_df,
    parse_and_save_xml,
)

API_URL = "https://mediathekviewweb.de/api/query"
TIMEOUT = 10

def test_call_mv_api_success():
    query = {"query": "test"}
    response_data = {"result": {"results": [{"id": 1, "title": "Test"}]}}
    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = response_data
        response = _call_mv_api_(API_URL, query)
        assert response == response_data

def test_call_mv_api_failure():
    query = {"query": "test"}
    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 500
        response = _call_mv_api_(API_URL, query)
        assert response is None

def test_get_program_success():
    program = "Test Program"
    query = {"query": "test"}
    response_data = {"result": {"results": [{"timestamp": 1609459200, "url_subtitle": "https://example.com/subtitle"}]}}
    with patch("oer_scraper.scraper._call_mv_api_", return_value=response_data):
        df = get_program(program, query)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

def test_get_program_no_data():
    program = "Test Program"
    query = {"query": "test"}
    with patch("oer_scraper.scraper._call_mv_api_", return_value=None):
        df = get_program(program, query)
        assert df is None

def test_save_metadata(tmp_path):
    program = "Test Program"
    date = "2021-01-01"
    data = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    save_metadata(program, date, data, tmp_path)
    assert (tmp_path / f"{program}/{date}/{program}_{date}.csv").exists()

def test_zip_folder(tmp_path):
    folder_path = tmp_path / "test_folder"
    folder_path.mkdir()
    (folder_path / "test_file.txt").write_text("test")
    zip_path = tmp_path / "test.zip"
    zip_folder(folder_path, zip_path)
    assert zip_path.exists()

def test_get_path(tmp_path):
    program = "Test Program"
    date = "2021-01-01"
    path = get_path(program, date, tmp_path)
    assert path.exists()

def test_save_subtitles(tmp_path):
    df = pd.DataFrame({"url_subtitle": ["https://example.com/subtitle"], "permanent_id": ["test_id"]})
    program = "Test Program"
    date = "2021-01-01"
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = b"<xml></xml>"
        result_df = save_subtitles(df, program, date, tmp_path)
        assert "xml_path" in result_df.columns
        assert (tmp_path / f"{program}/{date}/xml-subtitles/test_id.xml").exists()

def test_download_video_success(tmp_path):
    url = "https://example.com/video"
    file_name = tmp_path / "video.mp4"
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.iter_content = lambda chunk_size: [b"data"]
        success = download_video(file_name, url)
        assert success
        assert file_name.exists()

def test_download_video_failure():
    url = "https://example.com/video"
    file_name = "video.mp4"
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 404
        success = download_video(file_name, url)
        assert not success

def test_download_videos_as_zip(tmp_path):
    df = pd.DataFrame({"url_video_low": ["https://example.com/video"], "permanent_id": ["test_id"]})
    program = "Test Program"
    date = "2021-01-01"
    with patch("oer_scraper.scraper.download_video", return_value=True):
        download_videos_as_zip(program, date, df, tmp_path, zipped=True)
        assert (tmp_path / f"{program}/{date}/videos/test_id.mp4").exists()
        assert (tmp_path / f"{program}/{date}.zip").exists()

def test_xml_to_df():
    xml_content = """
    <tt xmlns="http://www.w3.org/ns/ttml">
        <body>
            <div>
                <p begin="00:00:01.000" end="00:00:02.000">
                    <span>Test subtitle</span>
                </p>
            </div>
        </body>
    </tt>
    """
    xml_path = pathlib.Path("test.xml")
    xml_path.write_text(xml_content)
    df = xml_to_df(xml_path)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    xml_path.unlink()

def test_parse_xml_df():
    df = pd.DataFrame({
        "text": ["Test subtitle."],
        "color": [""],
        "start_time": ["00:00:01.000"],
        "end_time": ["00:00:02.000"]
    })
    parsed_df = parse_xml_df(df)
    assert isinstance(parsed_df, pd.DataFrame)
    assert not parsed_df.empty

def test_parse_and_save_xml(tmp_path):
    df = pd.DataFrame({"xml_path": ["test.xml"], "permanent_id": ["test_id"]})
    xml_content = """
    <tt xmlns="http://www.w3.org/ns/ttml">
        <body>
            <div>
                <p begin="00:00:01.000" end="00:00:02.000">
                    <span>Test subtitle</span>
                </p>
            </div>
        </body>
    </tt>
    """
    xml_folder = tmp_path / "Test Program/2021-01-01/xml-subtitles"
    xml_folder.mkdir(parents=True)
    xml_path = xml_folder / "test.xml"
    xml_path.write_text(xml_content)
    parse_and_save_xml("Test Program", "2021-01-01", tmp_path, df)
    assert (tmp_path / "Test Program/2021-01-01/subtitles/test_id.csv").exists()
    xml_path.unlink()
