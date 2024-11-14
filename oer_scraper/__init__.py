"""This is OER Scraper.

With this small tool you can scrape TV programs from the German public
broadcasters' mediatheks.

Usage
=====

You can use the command line interface to scrape the programs.
The following example shows how to scrape the programs from the last week and
download the videos:

$ oerscraper --help
Usage: oerscraper [OPTIONS] OUTPUT_FOLDER

Options:
  --subtitles / --no-subtitles    Include subtitles. Default: Yes.
  --parse / --no-parse            Parse the subtitles. Default: Yes.
  --download / --no-download      Download the videos. Default: No.
  -S, --start_time [%Y-%m-%d|%Y-%m-%dT%H:%M:%S|%Y-%m-%d %H:%M:%S]
  -E, --end_time [%Y-%m-%d|%Y-%m-%dT%H:%M:%S|%Y-%m-%d %H:%M:%S]
  -I, --interval INTEGER          Interval in days
  -c, --config FILENAME           Configuration file in YAML format
  --help                          Show this message and exit.

The configuration file should look like this:
    
```yaml
tagesschau:
  queries:
    - fields: ["title", "topic"]
      query: "tagesschau"
    - fields: ["channel"]
      query: "ard"
  sortBy: "timestamp"
  sortOrder: "desc"
  min_duration: 300
  future: false
  offset: 0
  size: 8000
```

The configuration file specifies the queries to be made to the API.
The scraper will download all programs that match the queries.
The configuration file can contain multiple programs.
You can specify more than one program/query in the configuration file. 
The scraper will download all programs that match any of the queries.

"""

__version__ = "0.1.0"
