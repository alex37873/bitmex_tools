import os
import subprocess
from datetime import datetime, timedelta
from typing import List

import requests
from settings import CALLBACK, DOWNLOAD_FOLDER


def download_file(url: str, filename: str) -> None:
    response = requests.get(url=url, timeout=30)

    if response.status_code == 200:
        file_path = os.path.join(DOWNLOAD_FOLDER, filename)
        with open(file=file_path, mode="wb") as f:
            f.write(response.content)
        print(f"Downloaded {filename}")
    else:
        print(f"Failed to download {filename}")


def get_date_range(start_date: str, end_date: str) -> List[str]:
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    delta = end - start

    return [
        (start + timedelta(days=i)).strftime("%Y%m%d") for i in range(delta.days + 1)
    ]


def cleanup_download_folder() -> None:
    for filename in os.listdir(DOWNLOAD_FOLDER):
        if filename.endswith(".csv.gz"):
            file_path = os.path.join(DOWNLOAD_FOLDER, filename)
            try:
                os.unlink(path=file_path)
                print(f"Deleted {filename}")
            except Exception as e:
                print(f"Failed to delete {filename}: {e}")


def main(date_arg: str, folder: str = "trade", cleanup: bool = False) -> None:
    base_url = f"https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/{folder}/"

    if cleanup:
        cleanup_download_folder()

    if "-" in date_arg:
        start_date, end_date = date_arg.split("-")
        dates = get_date_range(start_date=start_date, end_date=end_date)
    else:
        dates = [date_arg]

    for date in dates:
        url = f"{base_url}{date}.csv.gz"
        filename = f"{date}.csv.gz"
        download_file(url=url, filename=filename)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python script.py <date or date range> [cleanup]")
        sys.exit(1)

    date_arg = sys.argv[1]
    cleanup = sys.argv[2] == "cleanup" if len(sys.argv) == 3 else False

    os.makedirs(name=DOWNLOAD_FOLDER, exist_ok=True)

    main(date_arg=date_arg, cleanup=cleanup)  # or folder="quote"

    subprocess.run(args=CALLBACK, check=True)
