#! /usr/bin/env python3
 
import time
from pathlib import Path
 
 
def seconds_to_formatted_time_string(seconds: float) -> str:
    """
    Given the number of seconds, returns a formatted string showing the time
        duration
    """

    hour = int(seconds / (60 * 60))
    minute = int((seconds % (60 * 60)) / 60)
    second = seconds % 60

    return '{}:{:>02}:{:>05.2f}'.format(hour, minute, second)


def print_loop_status_with_elapsed_time(
    the_iter: int, every_nth_iter: int, total_iter: int, start_time: float):
    """
    Prints message providing loop's progress for user

    :param the_iter: index that increments by 1 as loop progresses
    :param every_nth_iter: message should be printed every nth increment
    :param total_iter: total number of increments that loop will run
    :param start_time: starting time for the loop, which should be
        calculated by 'import time; start_time = time.time()'
    """

    current_time = time.ctime(int(time.time()))

    every_nth_iter_integer = max(round(every_nth_iter), 1)

    if the_iter % every_nth_iter_integer == 0:
        print('Processing loop iteration {i} of {t}, which is {p:0f}%, at {c}'
              .format(i=the_iter + 1,
                      t=total_iter,
                      p=(100 * (the_iter + 1) / total_iter),
                      c=current_time))
        elapsed_time = time.time() - start_time

        print('Elapsed time: {}'.format(seconds_to_formatted_time_string(
            elapsed_time)))


def get_filepaths_in_directory_by_extension(
    directory_path: Path, file_extension: str) -> list[Path]:
    """
    Returns filepaths of parquet files in the data directory
    """

    file_extension = '.' + file_extension

    filepaths = [
        e for e in directory_path.iterdir() 
        if e.as_posix()[-len(file_extension):] == file_extension]

    return filepaths    


