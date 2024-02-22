#! /usr/bin/env python3

import time
import polars as pl
from pathlib import Path

from src.common.common import (
    print_loop_status_with_elapsed_time,
    get_filepaths_in_directory_by_extension,
    )


def read_time_temperature_df(
    df_filepath: Path, colnames: list[str], widths: list[int]) -> pl.DataFrame:
    """
    Read fixed-width table from text file

    'colnames':  table column names
    'widths':  column widths in terms of numbers of characters

    Adapted from:
        https://github.com/pola-rs/polars/issues/3151
        ghuls commented Jan 19, 2023
    """

    colname = 'table'
    df = pl.read_csv(df_filepath, has_header=False, new_columns=[colname])

    slice_tuples = []
    offset = 0

    for w in widths:
        slice_tuples.append((offset, w))
        offset += w

    df1 = df.with_columns(
        [pl.col(colname)
         .str.slice(slice_tuple[0], slice_tuple[1])
         .str.strip_chars()
         .alias(col)
         .cast(pl.Int16)
         for slice_tuple, col in zip(slice_tuples, colnames)]).drop(colname)

    df2 = df1.select(
        timestamp=pl.datetime(
            pl.col('year'), pl.col('month'), pl.col('day'), pl.col('hour'), 0, 0),
        temperature=pl.col('temperature'))

    return df2


def main01():
    """
    Given data tables provide weather data for each station for each year (i.e.,
        one table per station-year)
    This program compiles the data so that each station has one file that 
        encompasses all years
    """

    ##################################################

    input_path = Path.home() / 'Documents' / 'isd_lite'
    output_path = Path.home() / 'Documents' / 'isd_lite_compiled'
    output_path.mkdir(exist_ok=True, parents=True)

    input_extension = 'txt'
    existing_input_table_filepaths = get_filepaths_in_directory_by_extension(
        input_path, input_extension)

    years = range(1980, 2024)
    years_str = [str(e) for e in years]
    compile_filepaths = [
        e for e in existing_input_table_filepaths  
        if e.stem.split('-')[-1] in years_str]

    compile_filepaths.sort()

    colnames = ['year', 'month', 'day', 'hour', 'temperature']
    widths = [4, 3, 3, 3, 6]

    dfs = []

    loop_len = len(compile_filepaths)
    start_time = time.time()
    for i, e in enumerate(compile_filepaths):
        print_loop_status_with_elapsed_time(
            i, loop_len//100, loop_len, start_time)

        df = read_time_temperature_df(e, colnames, widths)
        dfs.append(df)

        current_station_id = '-'.join(e.stem.split('-')[:2])
        next_idx = min(i+1, len(compile_filepaths)-1)
        next_station_id = (
            '-'.join(compile_filepaths[next_idx].stem.split('-')[:2]))

        at_station_last_idx = current_station_id != next_station_id
        at_end_idx = i == (len(compile_filepaths)-1)
        if at_station_last_idx or at_end_idx:
            station_df = pl.concat(dfs).unique()
            output_filepath = output_path / (current_station_id + '.parquet')
            station_df.write_parquet(output_filepath)
            dfs = []


def main02():
    """
    Given data tables provide weather data for each station for each year (i.e.,
        one table per station-year)
    This program provides a single file that includes all years for all stations
    """

    input_path = Path.home() / 'Documents' / 'isd_lite_compiled'
    output_path = input_path
    output_filepath = output_path / 'station_temperatures.parquet'

    input_extension = 'parquet'
    station_table_filepaths = get_filepaths_in_directory_by_extension(
        input_path, input_extension)
    station_table_filepaths = [
        e for e in station_table_filepaths if e != output_filepath]

    # convert string to integer to reduce size of resulting table
    station_ids = [int(e.stem.replace('-', '')) for e in station_table_filepaths]
    stations_dfs = [
        pl.read_parquet(e).with_columns(station=station_ids[i])
        for i, e in enumerate(station_table_filepaths)]

    stations_df = pl.concat(stations_dfs)

    # delete list to reduce memory usage
    del stations_dfs 

    stations_df.write_parquet(output_filepath)
    stations_df.head(100).write_csv(
        output_path / ('station_temperatures' + '.csv'))


if __name__ == '__main__':
    main01()
    main02()
