from configparser import ConfigParser
import psycopg2 as pg

def config(filename='config/database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file.'.format(section, filename))

    return db

def connect_db():
    conn = None
    try:
        params = config()

        print('Connecting to Database...')
        conn = pg.connect(**params)

        cur = conn.cursor()
        cur.execute('SELECT version()')
        db_version = cur.fetchone()

        print('Connected to database version:')

        print(db_version)

    except (Exception, pg.DatabaseError) as error:
        print(error)

    return conn,cur