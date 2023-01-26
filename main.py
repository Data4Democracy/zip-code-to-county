from pathlib import Path
from datetime import datetime

import requests
import time
import pandas as pd

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
DOWNLOADS_DIR = DATA_DIR / "downloads"
HUD_DIR = DOWNLOADS_DIR / "hud"

HUD_URL_FMT = "https://www.huduser.gov/portal/datasets/usps/ZIP_COUNTY_{}.xlsx"
CENSUS_FIPS_URL = "https://www2.census.gov/geo/docs/reference/codes/files/national_county.txt"

# the FIPS data does not come with column names
CENSUS_DATA_COL_NAMES = ["STATE", "STATEFP", "COUNTYFP", "COUNTYNAME", "CLASSFP"]


def verify_downloads_dir_exists() -> None:
    paths = [DATA_DIR, DOWNLOADS_DIR, HUD_DIR]
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def remove_downloads_dir() -> None:
    def rmtree(path: Path) -> None:
        if path.is_file():
            path.unlink(missing_ok=True)
        else:
            files = list(path.iterdir())
            for child in files:
                rmtree(child)

            path.rmdir()

    rmtree(DOWNLOADS_DIR)


def process_hud_file(path: Path) -> pd.DataFrame:
    """Return processed HUD file as dataframe"""

    df = pd.read_excel(
        path,
        index_col=None,
        # some file have lowercase column names
        converters={"ZIP": str, "COUNTY": str, "zip": str, "county": str},
    )

    df.columns = df.columns.str.upper()

    # rename County column for easier merging.
    df = df.rename(columns={"COUNTY": "STCOUNTYFP"})

    # keep the two columns we need
    df = df[["ZIP", "STCOUNTYFP"]]

    return df

def process_census_fips_file(path: Path) -> pd.DataFrame:
    """Return processed Census FIPS as dataframe"""

    df = pd.read_csv(path,
        names=CENSUS_DATA_COL_NAMES,
        converters={"STATEFP": str, "COUNTYFP": str, "CLASSFP": str}
    )

    # Combine State & County FP to generate the full FIPS code for easier lookup
    df["STCOUNTYFP"] = df["STATEFP"] + df["COUNTYFP"]

    # Drop STATFP & COUNTYFP as we no longer need them
    df = df[["STCOUNTYFP", "STATE" ,"COUNTYNAME"]]

    return df


def download_file(url: str, file_name: Path | None= None) -> Path:
    """Download a file."""

    file_name = file_name or Path(url.split("/")[-1])

    resp = requests.get(url)

    if resp.status_code >= 400:
        raise RuntimeError("Error downloading file.")

    with file_name.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024):
            # filter out keep-alive new chunks
            if chunk:
                f.write(chunk)

    return file_name


def main():
    # the beginning of hud year data
    # START_YEAR = 2010
    START_YEAR = 2021
    # hud files are based on quarters
    MONTHS = ["03", "06", "09", "12"]
    KEEP_DOWNLOADS = True

    # Get current year to handle future runs of this file
    cur_year = datetime.now().year

    file_name = CENSUS_FIPS_URL.split("/")[-1]
    fips_file = download_file(
        CENSUS_FIPS_URL,
        file_name=(DOWNLOADS_DIR / file_name).with_suffix(".csv")
    )
    fips_df = process_census_fips_file(fips_file)

    for year in range(START_YEAR, cur_year + 1):
        for month in MONTHS:
            hud_url = HUD_URL_FMT.format(f"{month}{year}")
            file_name = hud_url.split("/")[-1]

            try:
                hud_file = download_file(hud_url, file_name=HUD_DIR / file_name)
            except RuntimeError as e:
                if "Error downloading file." in str(e):
                    # once we get to a Q that hasn't happened yet, we'll get an XLDRerror
                    print(f"No data for {year}-{month}")
                    break
                else:
                    raise

            excel_df = process_hud_file(hud_file)

            csv_path = DATA_DIR / f"zip_county_state_{year}_{month}.csv"

            merged_df = fips_df.merge(excel_df)
            # keep specific columns
            merged_df = merged_df[["ZIP", "COUNTYNAME", "STATE", "STCOUNTYFP"]]
            merged_df.to_csv(csv_path, encoding="utf-8", index=False)

            # prevent from overloading the HUD site and to be a nice visitor
            time.sleep(1)
            print(f"Completed {csv_path}")

    if not KEEP_DOWNLOADS:
        print("Removing downloaded files.")
        remove_downloads_dir()


if __name__ == '__main__':
    verify_downloads_dir_exists()
    main()
