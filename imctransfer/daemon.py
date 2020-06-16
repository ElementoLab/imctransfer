#!/usr/bin/env python


"""
Query for MCD files hosted on Box.com.
Download them and write their metadata to disk.
"""


import sys
import argparse
import hashlib
from typing import List, Union, NoReturn
import json
import time
import logging
from pathlib import Path


import yaml
from boxsdk import OAuth2, Client, BoxOAuthException
from boxsdk.object.file import File as BoxFile
from boxsdk.object.folder import Folder as BoxFolder
import iso8601  # for date string -> date object  # import rfc3339  # for date object -> date string
import pandas as pd


# type aliases of Box.com's objs
Folder = Union[BoxFolder]
File = Union[BoxFile]


REFRESH_TIME = 2 * 60 * 60  # refresh time in seconds
SECRET_FILE = Path("~/.box.access_tokens.yaml").expanduser().absolute()
DB_FILE = Path("~/.imctransfer.urls.json").expanduser().absolute()
FILE_TYPE = "mcd"
PROJECT_DIR = Path(".").absolute()
METADATA_FILE = Path("metadata") / "annotation.auto.csv"
DATA_DIR = "data"


class Daemon:
    def __init__(self, client, log, args, fresh: bool = False):
        fresh = False
        self.client = client
        self.log = log
        self.args = args
        if fresh:
            self.clean_db()

    def run(self) -> NoReturn:
        """The main loop of the script."""
        while True:
            self.log.info("Querying for new files.")
            # urls = crawl_for_file_type(root_folder, file_type=self.file_type)
            urls = self.query_for_file_type()
            if self.get_db() != urls:
                self.log.info("Found new files.")
                self.get_metadata_and_data(urls)
                self.save_db(urls)
                self.log.info("Completed query.")
            else:
                self.log.info("Did not find new files.")
            time.sleep(self.args.refresh_time)

    def clean_db(self):
        """Remove the database file."""
        self.log.info("Removing previously existing database.")
        self.args.db_file.unlink(missing_ok=True)

    def query_for_file_type(self) -> List[str]:
        """
        Query Box.com user folder and its children for a file of the `file_type`
        and return their URLs.
        """
        ft = self.args.file_type
        items = self.client.search().query(f"*.{ft}", file_extensions=[ft])
        return [item.get_url() for item in items]

    def get_metadata_and_data(self, urls: List[str]) -> None:
        """
        Get the metadata for the Box.com files in the `urls`,
        build a dataframe with them and download.
        """
        _meta = dict()
        for url in urls:
            file = File(session=self.client.session, object_id=url.split("/")[-1])
            file = file.get(fields=["name", "created_at", "created_by", "file_version"])  # type: ignore

            print(file.name)
            name = file.name.replace(".mcd", "").replace(" ", "_")
            dt = iso8601.parse_date(file.created_at)

            output_file = self.args.data_dir / name / file.name
            downloaded = False
            mismatch = False
            if self.args.download:
                output_file.parent.mkdir(exist_ok=True, parents=True)
                if output_file.exists():
                    mismatch = self.get_sha1(output_file) != file.file_version.sha1
                    if mismatch:
                        self.log.info("File exists but SHA1 has does not match. Re-downloading.")
                if not output_file.exists() or self.args.overwrite or mismatch:
                    self.log.info("Downloading '%s' to '%s'.", name, output_file)
                    self.download_file(file, output_file)
                    if self.get_sha1(output_file) == file.file_version.sha1:
                        downloaded = True
                        self.log.info("SHA1 matches.")
                    else:
                        self.log.error("SHA1 mismatch - will delete file.")
                        output_file.unlink()

            _meta[name] = {
                "sample_name": name,
                "mcd_file": file.name,
                "created_by": file.created_by.name,
                "created_at": dt.isoformat(),
                "url": url,
                "sha1": file.file_version.sha1,
                "downloaded": downloaded,
                "written_to": output_file if downloaded else None,
            }

        if not self.args.metadata:
            return
        meta = pd.DataFrame(_meta).T
        if meta.empty:
            return
        self.log.info("Saving metadata.")
        meta["acquisition_date"] = meta["sample_name"].str.extract(r"^(20\d{6}).*")[0]
        meta.sort_values("acquisition_date").to_csv(self.args.metadata_file, index=False)

    # async def download_file(self, file: File, output_file: Path) -> None:
    #     await file.download_to(open(output_file.absolute(), "wb"))

    def download_file(self, file: File, output_file: Path) -> None:
        """Download `file` from Box.com to `output_file` in local disk."""
        try:
            file.download_to(open(output_file, "wb"))
        except KeyboardInterrupt:
            output_file.unlink()
            raise
        self.log.info("Dowload completed.")

    @staticmethod
    def get_sha1(file: Path, buffer_size: int = 65536) -> str:
        """Calculate the sha1 hash of `file`."""
        sha1 = hashlib.sha1()

        with open(file, "rb") as f:
            while True:
                data = f.read(buffer_size)
                if not data:
                    break
                sha1.update(data)
        return sha1.hexdigest()

    def get_db(self) -> List[str]:
        """Load database from disk."""
        try:
            return json.load(open(self.args.db_file, "r"))
        except FileNotFoundError:
            return []

    def save_db(self, obj: List[str]) -> None:
        """Serialize database to disk."""
        json.dump(obj, open(self.args.db_file, "w"))


def argument_parser() -> argparse.ArgumentParser:
    """The argument parser for the script."""
    parser = argparse.ArgumentParser()
    _vars = ["client_id", "client_secret", "access_token"]
    hlp = f"YAML file with 3 variables: {', '.join(_vars)}. Defaults to '{SECRET_FILE}'."
    parser.add_argument("--secrets", dest="secrets_file", default=SECRET_FILE, type=Path, help=hlp)
    hlp = f"Database file. Defaults to '{DB_FILE}'."
    parser.add_argument("--db", dest="db_file", default=DB_FILE, type=Path, help=hlp)
    hlp = "Do not save CSV metadata."
    parser.add_argument("--no-metadata", dest="metadata", action="store_false", help=hlp)
    hlp = "Do not save MCD files."
    parser.add_argument("--no-mcd", dest="download", action="store_false", help=hlp)
    hlp = "Whether to ovewrite MCD files in disk."
    parser.add_argument("--overwrite", dest="overwrite", action="store_true", help=hlp)
    hlp = "Ignore previous queries and start anew. Will delete previous database."
    parser.add_argument("--fresh", dest="fresh", action="store_true", help=hlp)
    hlp = "File type ending to look for. Defaults to 'mcd'."
    parser.add_argument("-e", "--file-ending", dest="file_type", default=FILE_TYPE, help=hlp)
    hlp = "Time in between crawls. Default is 2 hours."
    parser.add_argument("-r", "--refresh-time", default=REFRESH_TIME, type=int, help=hlp)
    # hlp = "Name of Box.com uploading user to restrict query to."
    # parser.add_argument("-u", "--user", type=str, help=hlp)
    hlp = f"Path to output metadata file. Defaults to '`project_dir`/{METADATA_FILE}'."
    parser.add_argument("-m", "--metadata-file", type=Path, help=hlp)
    hlp = f"Parent directory to write MCD files to. Defaults to '`project_dir`/{DATA_DIR}/'."
    parser.add_argument("-d", "--data-dir", type=Path, help=hlp)
    hlp = f"Root project directory to write files to. Defaults to '{PROJECT_DIR}'."
    parser.add_argument(
        "-o", "--output-dir", dest="project_dir", default=PROJECT_DIR, type=Path, help=hlp
    )
    return parser


def setup_logger(name="imctransfer", level=logging.INFO) -> logging.Logger:
    """The logger for the script."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def main() -> NoReturn:
    """Main entry point of the script."""
    # Get a logger
    log = setup_logger()

    # Parse args and set default files/dirs
    args = argument_parser().parse_args()
    if args.metadata_file is None:
        args.metadata_file = args.project_dir / METADATA_FILE
    if args.data_dir is None:
        args.data_dir = args.project_dir / DATA_DIR
    args.metadata_file.parent.mkdir(exist_ok=True, parents=True)
    args.data_dir.mkdir(exist_ok=True, parents=True)

    # Setup box.com connection
    log.info("Reading credentials and setting up connection with server.")
    secret_params = yaml.safe_load(open(args.secrets_file, "r"))
    oauth = OAuth2(**secret_params)
    client = Client(oauth)

    # Initialize daemon
    daemon = Daemon(client=client, log=log, args=args)

    log.info("Starting daemon.")
    try:
        sys.exit(daemon.run())
    except KeyboardInterrupt:
        log.info("User interrupted. Terminating.")
        sys.exit(0)
    except BoxOAuthException:
        log.error("Could not establish connection with server.")
        sys.exit(1)


if __name__ == "__main__":
    main()
