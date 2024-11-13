import click
import pathlib
import yaml
import typing

from pydantic.dataclasses import dataclass
from datetime import datetime
from tqdm import tqdm

from .scraper import get_program, save_metadata, save_subtitles, download_videos_as_zip, nachrichtensendungen, parse_and_save_xml


@dataclass
class QuerySpec:
    fields: typing.List[str]
    query: str


@dataclass
class ProgramQuery:
    queries: typing.List[QuerySpec]
    sortBy: typing.Literal["timestamp"]
    sortOrder: typing.Literal["desc"]
    future: int
    offset: int
    size: int


@dataclass
class Configuration:
    subtitles: bool
    parse: bool
    download: bool
    start_time: datetime
    end_time: datetime
    interval: int
    output_folder: pathlib.Path
    programs: typing.Dict[str, ProgramQuery]


@click.command()
@click.option('--subtitles/--no-subtitles', is_flag=True, help='Include subtitles', default=True)
@click.option('--parse/--no-parse', is_flag=True, help='Parse the subtitles', default=True)
@click.option('--download/--no-download', is_flag=True, help='Download the videos', default=False)
@click.option('--start_time', '-S', type=click.DateTime())
@click.option('--end_time', '-E', type=click.DateTime())
@click.option('--interval', '-I', type=int, help='Interval in days', default=7)
@click.option('--config', '-c', type=click.File(mode="rt", encoding="utf8"), help='Configuration file in YAML format')
@click.argument('output_folder', type=click.Path(
    file_okay=False,
    dir_okay=True,
    writable=True,
    resolve_path=True,
    path_type=pathlib.Path

))
def cli(subtitles, parse, download, start_time, end_time, interval, config, output_folder):
    # Step 0: setup environment.
    working_dir = output_folder or pathlib.Path.cwd()
    DATE = datetime.today().date().isoformat()
    configuration = {
        "subtitles": subtitles,
        "parse": parse,
        "download": download,
        "start_time": start_time,
        "end_time": end_time,
        "interval": interval,
        "output_folder": output_folder
    }
    # Step 1: read configuration and iterate the list of configured programs:
    configuration["queries"] = yaml.safe_load(config)
    # Step 1: validate inputs
    configuration = Configuration(**configuration)
    main(configuration, working_dir, DATE)

def main(configuration: Configuration, working_dir, DATE):
    for program, program_query in tqdm( configuration.programs.items(), desc="Scrape News", total = len(nachrichtensendungen)):
        data = get_program(program, program_query)  # Get current listing for the specified program.
        # Dump this into the SQLite database, deduplicate the data and decide from that whether to download additional data.
        save_metadata(program, DATE, data)
        if configuration.subtitles:
            save_subtitles(df, program, DATE)
        if configuration.parse:
            parse_and_save_xml(program, DATE)
        if configuration.download:
            download_videos_as_zip(working_dir, start_time, end_time)
