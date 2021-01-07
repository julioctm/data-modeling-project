import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure do a ETL process in the song file that is in the filepath provided as an argument.
    It first read a json file within the filepath, using the pandas module.
    Then it extract the song information in order to load it into the songs table.
    And finally, it extract the artist information in order to load it into the artists table.
    
    INPUTS:
    * cur - The cursor variable.
    * filepath - The filepath to the song file.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This procedure do a ETL process in the log file that is in the filepath provided as an argument.
    It first read a json file within the filepath, using the pandas module.
    Then it do filter the field page, looking for the argument 'NextSong'.
    And finally, it converts the timestamp column to datetime.
    
    In order to load the data into the tables:
    The first one to be loaded is the time table.
    Then the user table is loaded.
    And finally the songplay table is loaded.
    
    INPUTS:
    * cur - The cursor variable.
    * filepath - The filepath to the song file.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong'].reset_index()

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = [df.ts, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ('ts','hour','day','week','month','year','dayofweek')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This procedure takes the name of all the files that have JSON extension, from the filepath used as an argument.
    Then it counts the total number of files found and print it.
    And finally it print the amount of files that was processed with success.
    
    INPUTS:
    * cur - The cursor variable.
    * conn - The connection to database variable.
    * filepath - The filepath to the song file.
    * func - The variable that calls the song and log file process
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    The main process is reponsible for open the connection to database.
    Set the cursor to the cur variable.
    Call the process_data function.
    And at the end, close the connection to database.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()