import click
import pathlib
from tqdm import tqdm

from .scraper import get_sendung, save_metadata, save_subtitles, download_videos_as_zip, nachrichtensendungen, parse_and_save_xml

@click.command()
@click.option('--subtitles', '-s', is_flag=True, help='Include subtitles', default=True)
@click.option('--parse', '-p', is_flag=True, help='Parse the subtitles', default=True)
@click.option('--download', '-d', is_flag=True, help='Download the videos', default=False)
@click.option('--start_time', '-S', type=click.DateTime())
@click.option('--end_time', '-E', type=click.DateTime())
@click.option('--interval', '-I', type=int, help='Interval in days', default=7)
@click.argument('output_folder', type=click.Path(
    file_okay=False,
    dir_okay=True,
    writable=True,
    resolve_path=True,
    path_type=pathlib.Path

))
def cli(subtitles, parse, download, start_time, end_time, output_folder):
    working_dir = output_folder or pathlib.Path.cwd()
    DATE = start_time.date().isoformat()
    for sendung in tqdm(nachrichtensendungen, desc="Scrape News", total = len(nachrichtensendungen)):
        data = get_sendung(start_time, end_time)
        save_metadata(sendung, DATE, data)
        if subtitles:
            save_subtitles(df, sendung, DATE)
        if parse:
            parse_and_save_xml(sendung, DATE)
        if download:
            download_videos_as_zip(working_dir, start_time, end_time)


BASE_PATH = "./Broadcast_Subtitles/"
        