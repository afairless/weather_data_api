#! /usr/bin/env python3

import polars as pl
from pathlib import Path


def read_fixed_width_df(
    df: pl.DataFrame, schema_dict: dict[str, pl.PolarsDataType], 
    widths: list[int]) -> pl.DataFrame:
    """
    Read fixed-width table from text stored in a single-column dataframe ('df')

    'schema_dict':  keys are the table's column names; values are the columns'
        data types
    'widths':  list of column widths in terms of numbers of characters

    Adapted from:
        https://github.com/pola-rs/polars/issues/3151
        ghuls commented Jan 19, 2023
    """

    assert len(widths) == len(schema_dict)

    colname = 'table'
    df.columns = [colname]

    slice_tuples = []
    offset = 0

    for w in widths:
        slice_tuples.append((offset, w))
        offset += w

    df1 = (
        df.with_columns(
            [pl.col(colname)
             .str.slice(slice_tuple[0], slice_tuple[1])
             .str.strip_chars()
             .alias(col) 
             for slice_tuple, col in zip(slice_tuples, schema_dict.keys())])
            .drop(colname))

    all_data_types_valid = all([schema_dict[k] for k in schema_dict])
    if all_data_types_valid:
        df1 = pl.DataFrame(df1.to_dict(), schema=schema_dict)

    return df1


def extract_station_table_header(filepath: Path) -> list[str]:
    """
    Extract and return column names in table header
    """

    with open(filepath, 'r') as text_file:
        id_text = text_file.read()

    notes_end_idx = id_text.find(' file.')
    header_start_idx = id_text[notes_end_idx:].find('USAF')
    header_end_idx = id_text[notes_end_idx:].find('\n\n007018')
    header_text = id_text[
        notes_end_idx+header_start_idx:notes_end_idx+header_end_idx]
    header_text = header_text.lower().replace('station name', 'station_name')
    header_list = header_text.split(' ')
    header_list = [e for e in header_list if e]

    return header_list


def main():
    """
    Extract data from Integrated Surface Database Station History text file 
        'isd-history.txt' to create a table of weather station data, including
        stations' names, IDs, locations, and time periods of data collection

    'isd-history.txt' was downloaded from:
        ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.txt
    """


    # SET INPUT AND OUTPUT FILEPATHS
    ##################################################

    output_path = Path.cwd() / 'output'
    output_path.mkdir(exist_ok=True, parents=True)

    input_filepath = (
        Path.cwd() / 'data_documentation' / 'all_station_ids' / 
        'isd-history.txt')


    # EXTRACT AND FORMAT TABLE
    ##################################################

    colnames = extract_station_table_header(input_filepath)
    data_types = [None] * len(colnames)
    schema_dict = dict(zip(colnames, data_types))
    df_text = pl.read_csv(input_filepath, has_header=False, separator='|')
    df_text = df_text[22:]
    widths = [6, 6, 30, 5, 3, 6, 8, 9, 8, 9, 9]
    df = read_fixed_width_df(df_text, schema_dict, widths)

    df = df.select(
        pl.when(pl.col(pl.Utf8).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.Utf8))
        .name.keep())

    df = df.with_columns(
        pl.col('lat').cast(pl.Float32),
        pl.col('lon').cast(pl.Float32),
        pl.col('elev(m)').cast(pl.Float32),
        pl.col('begin').str.strptime(pl.Datetime, '%Y%m%d'),
        pl.col('end').str.strptime(pl.Datetime, '%Y%m%d'))


    #  SAVE FULL TABLE
    ##################################################

    output_filepath = output_path / 'all_stations.parquet'
    df.write_parquet(output_filepath)
    output_filepath = output_path / 'all_stations.csv'
    df.write_csv(output_filepath)


    #  SAVE FILTERED TABLE
    ##################################################

    filtered_df = df.filter(
        pl.col('begin') <= pl.date(2005, 1, 1),
        pl.col('end') >= pl.date(2024, 1, 1),
        pl.col('ctry') == 'US')

    output_filepath = output_path / 'stations_to_download.parquet'
    filtered_df.write_parquet(output_filepath)
    output_filepath = output_path / 'stations_to_download.csv'
    filtered_df.write_csv(output_filepath)


if __name__ == '__main__':
    main()
