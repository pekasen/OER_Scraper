import click
import pathlib
import yaml
import typing

from loguru import logger
from pydantic.dataclasses import dataclass
from dataclasses import asdict
from datetime import datetime
from tqdm import tqdm

from .scraper import get_program, save_metadata, save_subtitles, download_videos_as_zip, parse_and_save_xml


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
    min_duration: int


@dataclass
class Configuration:
    subtitles: bool
    parse: bool
    download: bool
    start_time: typing.Optional[datetime]
    end_time: typing.Optional[datetime]
    interval: typing.Optional[int]
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
    configuration["programs"] = yaml.safe_load(config)
    # Step 1: validate inputs
    configuration = Configuration(**configuration)
    main(configuration, working_dir, DATE)

@logger.catch
def main(configuration: Configuration, working_dir, DATE):
    for program, program_query in tqdm( configuration.programs.items(), desc="Scraping…"):
        data = get_program(program, asdict(program_query))  # Get current listing for the specified program.
        # Dump this into the SQLite database, deduplicate the data and decide from that whether to download additional data.
        if data is None or data.empty:
            continue
        save_metadata(program, DATE, data, configuration.output_folder)
        if configuration.subtitles:
            save_subtitles(data, program, DATE)
        if configuration.parse:
            parse_and_save_xml(program, DATE)
        if configuration.download:
            download_videos_as_zip(working_dir, start_time, end_time)
        return 0