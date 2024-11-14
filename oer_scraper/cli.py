"""Command line interface for the OER scraper."""

# pylint: disable=C0116,R0902,R0913,R0917

import pathlib
import typing
from dataclasses import asdict
from datetime import datetime, timedelta

import click
import yaml
from loguru import logger
from pydantic.dataclasses import dataclass
from tqdm import tqdm

from .scraper import (
    download_videos_as_zip,
    get_program,
    parse_and_save_xml,
    save_metadata,
    save_subtitles,
)


@dataclass
class QuerySpec:
    """Sepcifies a single query to be made to the API."""

    fields: typing.List[str]
    query: str


@dataclass
class ProgramQuery:
    """Specifies a program to be scraped."""

    # pylint: disable=C0103
    queries: typing.List[QuerySpec]
    sortBy: typing.Literal["timestamp"]
    sortOrder: typing.Literal["desc"]
    future: int
    offset: int
    size: int
    min_duration: int


@dataclass
class Configuration:
    """Configuration for the scraper."""

    subtitles: bool
    parse: bool
    download: bool
    start_time: typing.Optional[datetime]
    end_time: typing.Optional[datetime]
    interval: typing.Optional[int]
    output_folder: pathlib.Path
    programs: typing.Dict[str, ProgramQuery]


@click.command()
@click.option(
    "--subtitles/--no-subtitles",
    is_flag=True,
    help="Include subtitles. Default: Yes.",
    default=True,
)
@click.option(
    "--parse/--no-parse",
    is_flag=True,
    help="Parse the subtitles. Default: Yes.",
    default=True,
)
@click.option(
    "--download/--no-download",
    is_flag=True,
    help="Download the videos. Default: No.",
    default=False,
)
@click.option("--start_time", "-S", type=click.DateTime())
@click.option("--end_time", "-E", type=click.DateTime())
@click.option("--interval", "-I", type=int, help="Interval in days", default=7)
@click.option(
    "--config",
    "-c",
    type=click.File(mode="rt", encoding="utf8"),
    help="Configuration file in YAML format",
)
@click.argument(
    "output_folder",
    type=click.Path(
        file_okay=False,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
)
def cli(
    subtitles, parse, download, start_time, end_time, interval, config, output_folder
):
    # Step 0: setup environment.
    if end_time < start_time or interval < 0:
        raise ValueError("End time is before start time.")
    today = datetime.today().date().isoformat()
    configuration = {
        "subtitles": subtitles,
        "parse": parse,
        "download": download,
        "start_time": start_time,
        "end_time": end_time,
        "interval": interval,
        "output_folder": output_folder,
    }
    # Step 1: read configuration and iterate the list of configured programs:
    configuration["programs"] = yaml.safe_load(config)
    # Step 1: validate inputs
    configuration = Configuration(**configuration)
    main(configuration, today)


@logger.catch
def main(configuration: Configuration, today):
    for program, program_query in tqdm(
        configuration.programs.items(), desc="Scraping…"
    ):
        data = get_program(
            program, asdict(program_query)
        )  # Get current listing for the specified program.
        # Dump this into the SQLite database, deduplicate the data and decide
        # from that whether to download additional data.
        if data is None or data.empty:
            logger.warning(f"Quering for '{program}' has returned no data.")
            continue

        # print(data.loc[:, ["timestamp", "title", "url_subtitle"]])

        if configuration.start_time:
            start_time = configuration.start_time
            end_time = configuration.end_time or start_time - timedelta(
                days=configuration.interval
            )
            data = data.loc[
                data["timestamp"].between(start_time, end_time, inclusive="both"), :
            ]

            logger.info(
                f"Filtering data between {start_time} and {end_time}. Result has {len(data)} rows."
            )

        if configuration.subtitles:
            data = save_subtitles(data, program, today, configuration.output_folder)
        if configuration.parse:
            parse_and_save_xml(program, today, configuration.output_folder, data)
        if configuration.download:
            download_videos_as_zip(program, today, data, configuration.output_folder)
        save_metadata(program, today, data, configuration.output_folder)

    return 0
