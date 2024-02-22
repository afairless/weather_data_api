#! /usr/bin/env python3

import bs4
import gzip
import time
import asyncio
import aiohttp
import polars as pl
from pathlib import Path
from typing import Callable
from collections import Counter

from src.common.common import (
    print_loop_status_with_elapsed_time,
    get_filepaths_in_directory_by_extension,
    )


async def request_url_response_content(
    session: aiohttp.ClientSession, url: str) -> bytearray:
    """
    Asynchronously download URL response content
    If response is invalid, return empty result
    """
    async with session.get(url) as response:
        if response.ok:
            b_array = bytearray()
            while True:
                chunk = await response.content.read(1024)
                b_array.extend(chunk)
                if not chunk:
                    break
            return b_array
        else:
            return bytearray()


async def request_url_responses(
    urls: list[str], request_url: Callable, *args, **kwargs) -> list:
    """
    Returns asynchronously-downloaded URL responses 
    Accepts 'request_url' as a function that asynchronously downloads and 
        processes a single URL
    """

    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.ensure_future(request_url(session, url, *args, **kwargs)) 
            for url in urls]
        results = await asyncio.gather(*tasks)

    return results


def asynchronous_downloads(urls: list[str]) -> list[bytearray]:
    """
    Asynchronously download a series of URLs
    """

    downloads = asyncio.run(
        request_url_responses(urls, request_url_response_content))

    return downloads


def extract_file_list_from_html_bytes(html_bytes: bytes) -> list[str]:

    soup = bs4.BeautifulSoup(html_bytes.decode('utf-8'), 'html.parser')

    file_list = [
        e['href'] for e in soup.find_all('a', href=True) 
        if e.text and 'gz' in e.text]

    return file_list


def extract_file_list_from_year_webpages(
    base_url: str, years: range) -> list[str]:

    year_urls = [base_url + str(year) for year in years]
    year_webpages = asynchronous_downloads(year_urls)
    file_lists = [extract_file_list_from_html_bytes(e) for e in year_webpages]
    file_list = [e for sublist in file_lists for e in sublist]

    return file_list


'''
def get_file_list_from_urls_by_year(year_urls: list[str]) -> list[str]:
    year_webpages = asynchronous_downloads(year_urls)

    file_list = []
    for y in year_webpages:
        file_list_by_year = extract_file_list_from_html_bytes(y)
        file_list.extend(file_list_by_year)

    return file_list
'''


def save_compressed_bytes_to_text_file(b: bytes, filepath: Path):
    text = gzip.decompress(b).decode('utf-8')
    with open(filepath, 'w') as text_file:
        text_file.write(text)


def download_and_save_urls(
    file_urls: list[str], to_download_filepaths: list[Path]) -> float:

    downloads = asyncio.run(
        request_url_responses(file_urls, request_url_response_content))

    empty_download_n = [len(e) for e in downloads].count(0)
    current_time = time.ctime(int(time.time()))
    print(f'{current_time}: {empty_download_n} out of {len(downloads)} were empty')

    for i, e in enumerate(downloads):
        if len(e) > 0:
            save_compressed_bytes_to_text_file(e, to_download_filepaths[i])

    empty_proportion = empty_download_n / len(downloads)

    return empty_proportion  


def main():
    """
    Download Integrated Surface Database (ISD) Lite files containing historical 
        weather data from the National Oceanic and Atmospheric Administration 
        (NOAA) and the National Centers for Environmental Information (NCEI) 
    """


    # GET STATION LIST AND SET TIME RANGE OVER WHICH TO DOWNLOAD DATA
    ##################################################

    input_path = Path.cwd() / 'output'
    input_filepath = input_path / 'stations_to_download.parquet'
    station_df = pl.read_parquet(input_filepath)

    station_list = (
        station_df.select(
            pl.concat_str(pl.col('usaf'), pl.lit('-'), pl.col('wban')))
        .to_series()
        .to_list())

    max_year = station_df['end'].dt.year().max()
    # min_year = station_df['begin'].dt.year().max()
    min_year = 1980
    assert isinstance(max_year, int)
    assert isinstance(min_year, int)
    years = range(min_year, max_year)


    # GET LIST OF ALL FILES FOR SELECTED YEARS
    ##################################################

    base_url = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/'
    file_list = extract_file_list_from_year_webpages(base_url, years)


    # FILTER OUT ALREADY-DOWNLOADED FILES FROM TO-DOWNLOAD LIST
    ##################################################

    output_path = Path.home() / 'Documents' / 'isd_lite'
    output_path.mkdir(exist_ok=True, parents=True)

    output_extension = 'txt'
    existing_output_filepaths = get_filepaths_in_directory_by_extension(
        output_path, output_extension)

    existing_output_filestems = [e.stem for e in existing_output_filepaths]
    all_download_filestems = [
        e.split('.')[0] 
        for e in file_list 
        if '-'.join(e.split('-')[:2]) in station_list]
    to_download_filestems = list(
        set(all_download_filestems) - set(existing_output_filestems))
    to_download_filepaths = [
        output_path / (e + '.' + output_extension) 
        for e in to_download_filestems]


    # PRINT NUMBER OF DOWNLOADED FILES BY YEAR
    ##################################################

    all_files_by_year = [e.split('-')[-1] for e in all_download_filestems]
    existing_files_by_year = [
        e.stem.split('-')[-1] for e in existing_output_filepaths]
    all_files_count = Counter(all_files_by_year)
    existing_files_count = Counter(existing_files_by_year)

    print('\n')
    print('Number of all files by year')
    print(all_files_count)
    print('Number of existing files by year')
    print(existing_files_count)
    print('\n')


    # DOWNLOAD FILES
    ##################################################

    download_extension = 'gz'
    file_urls = [
        base_url + '/' + 
        f.stem.split('-')[-1] + '/' + 
        f.stem + '.' + download_extension 
        for f in to_download_filepaths]

    assert len(file_urls) == len(to_download_filepaths)
    batch_size = 12
    idxs = list(range(0, len(file_urls), batch_size))
    if idxs[-1] < (len(file_urls) - 1):
        idxs.extend([len(file_urls) - 1])

    time_delay = 15
    loop_len = len(idxs) - 1
    start_time = time.time()
    for i in range(loop_len):
        print_loop_status_with_elapsed_time(
            i, loop_len//100, loop_len, start_time)
        start_idx = idxs[i]
        end_idx = idxs[i+1]
        empty_proportion = download_and_save_urls(
            file_urls[start_idx:end_idx], 
            to_download_filepaths[start_idx:end_idx])

        if empty_proportion > 0.5:
            time_delay += 15
        time.sleep(time_delay)


if __name__ == '__main__':
    main()
