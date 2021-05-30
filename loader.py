import argparse, os, shapefile, pygeoif
from datetime import date
from config.config import connect_db
from sql import queries as q

parser = argparse.ArgumentParser(description='Load Boulder, CO daily parcel data and shapefiles.')
parser.add_argument('-l', '--latest',
                    dest='load_latest',
                    action='store_true',
                    help='Load the most current data available.')
parser.add_argument('-d', '--date',
                    dest='load_date',
                    action='store',
                    default='current_date',
                    help='Load a specific date (format yyyy-mm-dd) or \'all\' to fix missing data. Default is current_date.')
parser.add_argument('-o', '--overwrite',
                    dest='overwrite',
                    action='store_true',
                    help='Overwrite data in tables if exists.')
parser.add_argument('-r','--rebuild',
                    dest='rebuild',
                    action='store_true',
                    help='Rebuilds analytic tables from raw data.')

args = parser.parse_args()

def import_owner_data(db,cur,files,overwrite):

    for file in files:

        # verify folder isn't empty
        if not os.path.exists('./data/{}/Owner_Address.csv'.format(file)):
            print('No owner data in folder {} ...  skipping'.format(file))
            continue

        # check existing data
        cur.execute(q.owner_check_data.format(file))
        if cur.fetchone() and not overwrite:
            print('Data for file {} already exists... skipping'.format(file))
            continue

        # create temp table
        cur.execute(q.owner_create_temp_table)

        print('Loading owner data from folder {} ...'.format(file))
        # load temp table
        with open('./data/{}/Owner_Address.csv'.format(file), 'r') as f:
            cur.copy_expert(sql=q.owner_copy_data, file=f)

        # add file_index column
        cur.execute(q.owner_add_file_index.format(file))
        print('Done.')

        # if overwrite, delete from raw table
        if overwrite:
            print('Scrubbing old data')
            cur.execute(q.owner_clean_raw_table.format(file))

        # insert into raw table
        cur.execute(q.owner_load_raw_table)

        # drop temp table
        cur.execute(q.owner_drop_temp_table)

        db.commit()
        print('File {} completed'.format(file))


    print('Owner data loaded.')

def import_shapefile(db,cur,files,overwrite):

    for file in files:

        # verify folder isn't empty
        if not os.path.exists('./data/{}/Parcels/Parcels.shp'.format(file)):
            print('No shapefile data in folder {} ... skipping'.format(file))
            continue

        # check exiting data
        cur.execute(q.parcel_check_data.format(file))
        if cur.fetchone() and not overwrite:
            print('Data for file {} already exists... skipping'.format(file))
            continue

        # create temp table
        cur.execute(q.parcel_create_temp_table)

        print('Loading shapefile data from folder {} ...'.format(file))
        # load temp table
        sf = shapefile.Reader('./data/{}/Parcels/Parcels'.format(file))

        shape_records = sf.shapeRecords()
        skip_count = 0

        for record in shape_records:
            if record.shape.points:
                object_id = record.record['OBJECTID']
                parcel_no = record.record['PARCEL_NO']
                shape_area = record.record['SHAPEarea']
                shape_len = record.record['SHAPElen']
                shape = str(pygeoif.geometry.as_shape(record.shape))

                cur.execute(q.parcel_load_data.format(object_id, parcel_no, shape_area, shape_len, shape, file))
            else:
                skip_count += 1

        print('Records Skipped: {}'.format(skip_count))

        if overwrite:
            print('Scrubbing old data')
            cur.execute(q.parcel_clean_raw_table.format(file))

        # insert into raw table
        cur.execute(q.parcel_load_raw_table)

        # drop temp table
        cur.execute(q.parcel_drop_temp_table)

        db.commit()
        print('File {} completed'.format(file))

    print('Shapefile data loaded.')



def load_analytic_tables(db,cur):
    print('Building analytic tables...')
    cur.execute(q.analytic_rebuild)

    db.commit()
    print('Analytic tables rebuilt.')


def main(args):

    # connect to postgres
    db, cur = connect_db()

    files = [date.today().strftime('%Y%m%d')]
    valid_date = True
    filenames = []
    for dir in os.scandir('./data'):
        if not dir.name.startswith('.'):
            filenames.insert(0,dir.name)

    if args.load_latest:
        files = [max(filenames)]
    elif args.load_date == 'all':
        files = filenames
    elif args.load_date != 'current_date':
        try:
            year,month,day = args.load_date.split('-')
            date(int(year),int(month),int(day))
        except:
            valid_date = False
        files =  [args.load_date.replace('-','')]

    # import data
    if valid_date:
        print('Loading owner data...')
        import_owner_data(db,cur,files,args.overwrite)
        import_shapefile(db,cur,files,args.overwrite)
    else:
        print('{} is not a valid date. Exiting Raw Load'.format(args.load_date))


    # rebuild analytic tables
    if args.rebuild:
        load_analytic_tables(db,cur)

    # close db connection
    cur.close()
    db.close()

if __name__ == '__main__':
    main(args)