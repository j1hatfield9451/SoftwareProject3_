import requests
import sqlite3
import json

def convert_obj_to_string(dict_record,target_key):
    #converts dictionary to string
    if dict_record.get(target_key, None) is None:
        return None
    key_value = dict_record[target_key]
    value_string = json.dumps(key_value)
    return value_string

def main():
    response = requests.get("https://data.nasa.gov/resource/gh4g-9sfh.json")
    json_data = response.json()

    if response.status_code != 200:
        print("GET request failed")
    elif response.status_code == 404:
        print("Error code 404")
    elif response.status_code == 401:
        print("Error code 401")
    else:
        print("GET request successful")


    db_connection = sqlite3.connect('meteorite_db.db')
    print(f'connected to database: {db_connection}')

    db_cursor = db_connection.cursor()
    print(f'created cursor {db_cursor} on connection {db_connection}')



    # (-lat -long lat long)
    bound_box_dict = {'Africa_MiddleEast_Meteorites': (-17.8, -35.2, 62.2, 37.6),
                      'Europe_Meteorites': (-24.1, 38.0, 32.1, 71.1),
                      'Upper_Asia_Meteorites': (33.0, 38.0, 190.4, 72.7),
                      'Lower_Asia_Meteorites': (63.0, -9.9, 154.0, 37.6),
                      'Australia_Meteorites': (112.9, -43.8, 154.3, -11.1),
                      'North_America_Meteorites': (-168.2, 12.8, -52.0, 71.5),
                      'South_America_Meteorites': (-81.2, -55.8, -34.4, 12.5)}

    #LABELS
    AFRICA = "Africa_MiddleEast_Meteorites"
    EUROPE = "Europe_Meteorites"
    UPPER_ASIA = "Upper_Asia_Meteorites"
    LOWER_ASIA = "Lower_Asia_Meteorites"
    AUSTRALIA = "Australia_Meteorites"
    NORTH_AMERICA = "North_America_Meteorites"
    SOUTH_AMERICA = "South_America_Meteorites"

    create_table(AFRICA, db_cursor)
    create_table(EUROPE, db_cursor)
    create_table(UPPER_ASIA, db_cursor)
    create_table(LOWER_ASIA, db_cursor)
    create_table(AUSTRALIA, db_cursor)
    create_table(NORTH_AMERICA, db_cursor)
    create_table(SOUTH_AMERICA, db_cursor)

    for record in json_data:

        if (record.get('reclat') is None) or (record.get('reclong') is None):
            continue

        #(-17.8, -35.2, 62.2, 37.6)
        elif (-17.8 <= float(record.get('reclong')) <= 62.2) and (-35.2 <= float(record.get('reclat')) <= 37.6):
            db_cursor.execute('''INSERT INTO Africa_MiddleEast_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)
                               ))

        #(-24.1, 38.0, 32.1, 71.1)
        elif (-24.1 <= float(record.get('reclong')) <= 32.1) and (38.0 <= float(record.get('reclat')) <= 71.1):
            db_cursor.execute('''INSERT INTO Europe_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)
                               ))

        #(33.0, 38.0, 190.4, 72.7)
        elif (33.0 <= float(record.get('reclong')) <= 190.4) and (38.0 <= float(record.get('reclat')) <= 72.7):
            db_cursor.execute('''INSERT INTO Upper_Asia_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)
                               ))

        #63.0, -9.9, 154.0, 37.6
        elif (63.0 <= float(record.get('reclong')) <= 154.0) and (-9.9 <= float(record.get('reclat')) <= 37.6):
            db_cursor.execute('''INSERT INTO Lower_Asia_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)
                               ))

        #112.9, -43.8, 154.3, -11.1
        elif (112.9 <= float(record.get('reclong')) <= 154.3) and (-43.8 <= float(record.get('reclat')) <= -11.1):
            db_cursor.execute('''INSERT INTO Australia_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)
                               ))

        #-168.2, 12.8, -52.0, 71.5
        elif (-168.2 <= float(record.get('reclong')) <= -52.0) and (12.8 <= float(record.get('reclat')) <= 71.5):
            db_cursor.execute('''INSERT INTO North_America_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)
                               ))

        #-81.2, -55.8, -34.4, 12.5
        elif (-81.2 <= float(record.get('reclong')) <= -34.4) and (-55.8 <= float(record.get('reclat')) <= 12.5):
            db_cursor.execute('''INSERT INTO South_America_Meteorites VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)
                               ))

    db_connection.commit()
    db_connection.close()


def create_table(table_name: str, cursor: sqlite3.Cursor):
    #creates table with the name of str
    command_string = f'CREATE TABLE IF NOT EXISTS {table_name} (name TEXT, mass TEXT, reclat TEXT, reclong TEXT);'
    cursor.execute(command_string)
    cursor.execute(f'DELETE FROM {table_name}')


if __name__ == '__main__':
    main()
