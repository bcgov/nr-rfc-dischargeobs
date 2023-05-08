import constants
import requests
import pendulum
import sys
import os


def get_hydro_data(local_filename):
    """this is going to make a web request to retrieve the hydrometric data
    """
    if not os.path.exists(local_filename):
    
        # NOTE the stream=True parameter below
        with requests.get(constants.SOURCE_HYDRO_DATA, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    #if chunk: 
                    f.write(chunk)

def extract_by_day(input_file: str, input_date, output_file):
    is_header = True
    header_str = None
    write_fh = open(output_file, 'w')
    cnter = 0
    with open(input_file, 'r') as fh:

        for line in fh:
            #print(line)
            if is_header:
                is_header = False
                header_str = line
            else:
                line = line.strip()
                line_list = line.split(',')

                # 2023-05-07T19:20:00-08:00
                date_str = line_list[1]
                date_str = date_str.replace('T', ' ')

                #print(f'date_str is: {date_str}')
                date_as_date = pendulum.from_format(date_str, 'YYYY-MM-DD HH:mm:ssZZ')
                if date_as_date > input_date:
                    write_fh.write(line + '\n')
            if not (float(cnter) % 1000.0):
                print(f'read {cnter} ...')
            cnter += 1
    write_fh.close()

if __name__ == '__main__':
    # 

    start_date_str = '2023-05-08 00:00:00-08:00'
    start_date = pendulum.from_format(start_date_str, 'YYYY-MM-DD HH:mm:ssZZ')

    output_file = 'todays_data.csv'


    local_filename = 'discharge_obs.csv'
    get_hydro_data(local_filename)
    print("constants.SOURCE_HYDRO_DATA is equal to " + constants.SOURCE_HYDRO_DATA)
    print(f'the source url is: {constants.SOURCE_HYDRO_DATA}')


    extract_by_day(local_filename, start_date, output_file)








