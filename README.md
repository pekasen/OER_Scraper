
# Public Broadcast Subtitle Downloader

This script downloads and processes metadata, subtitles, and videos for German public broadcast news shows and political talkshows from the [MediathekView](https://mediathekviewweb.de/) API. It allows users to fetch data, download videos, save subtitles, and parse subtitle XML files into structured formats.

- Tagesschau
- Tagesthemen
- ZDF Heute
- ZDF Heute Journal
- Caren Miosga
- Anne Will
- Hart aber Fair
- Markus Lanz
- Maybrit Illner
- Maischberger

## Features

- Fetches metadata for specified TV shows.
- Downloads subtitles in XML format.
- Parses subtitles from XML to CSV.
- Downloads and zips video files.
- Saves metadata to CSV files.

## Prerequisites

- Python 3.6+
- Required Python packages:
  - `requests`
  - `pandas`
  - `tqdm`

You can install the required packages using pip:

```bash
pip install -r requirements.txt
```

## Usage

The script can be run directly from the command line. You can specify whether to download subtitles, parse subtitles, and download videos using command-line arguments.

### Command-Line Arguments

- `--subtitles`: Download subtitles (1 for True, 0 for False). Default is `1`.
- `--parsed`: Parse subtitles (1 for True, 0 for False). Default is `1`.
- `--videos`: Download videos (1 for True, 0 for False). Default is `1`.

### Example

To run the script and download subtitles, parse them, and skip video downloads:

```bash
python Scraper.py --subtitles 1 --parsed 1 --videos 0
```
