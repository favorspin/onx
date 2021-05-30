# onX Data Engineering Project
Load Boulder, CO parcel and land ownership data and perform simple analysis of the results.

## Dependencies
- Python 3.8
	- configparser
	- psycopg2
	- argparse
	- os
	- shapefile
	- pygeoif
- PostgreSQL 13
	- PostGIS 3.1.1

## Setup 

### Edit database config file
Edit config file at `/config/database.ini` with your connection string. A sample is provided at `/config/database_sample.ini`

### Run initial SQL
Run the sql file found at `/sql/setup.sql` This will create initial tables that the script will use to load data into as well as create the PostGIS extension if you do not already have it set up.

### Download and stage data
Download the Owners and Addresses file from <https://opendata-bouldercounty.hub.arcgis.com/documents/bouldercounty::assessors-property-data/explore> 

Download the Shapefile Data from <https://opendata-bouldercounty.hub.arcgis.com/datasets/parcels/explore>

Extract the Shapefile data and move the resulting folder along and the Owners and Addresses file into the `data` directory in a folder titled with the date that the data was downloaded in `yyyyMMdd` format.

Example:

~~~
/
--> data/
	--> 20210526/
		--> Owner_Address.csv
		--> Parcels/
			--> Parcels.cpg
			--> Parcels.dbf
			--> Parcels.prj
			--> Parcels.shp
			--> Parcels.shx
			--> Parcels.xml
~~~

## Running the Loadfile

By default, running the script will only load data to raw tables from a data folder with the current days date:

`python loader.py`

Additional arguments can be passed to modify the jobs behavior.

- `--help` - Shows a list of arguments and their functions.
- `--latest` - loads the latest data in the data directory if the latest is not the current date. If `--latest` is used, `--date` is ignored.
- `--date <load_date>` loads a specifc date of data from the data directory. `load_date` should be in `yyyy-mm-dd` format. Alternatively, `all` can be specified for `load_date` to load all available data.
- `--overwrite` by default the script will skip any data that is already loaded into raw tables. This option will delete existing data and replace it with the requested data.
- `--rebuild` rebuilds analytics aggregate tables from data after loading the raw tables. This option will truncate the analytics tables and reload them entirely. If this option is not added, the analytics tables remain unchanged.

If running the job first time it is likely you'll want all data to be loaded and the analytics tables to be rebuilt. 

`python loader.py --date all --rebuild`

## Data Structure

This job loads data into two schemas:

- onx_raw
- onx_analytics

### `onx_raw` schema

The raw schema contains tables that hold raw data from the files. Small transformations are done during this stage, like removing invalid data, or replacing empty strings with `null`. There are no aggregations at this level.

The raw schema can be used by analysts intending on developing new metrics or needing to run adhoc exploratory analyses. While they could be used in reporting platforms, their intent is for internal development of metrics and would not, generally, be exposed to an end user.

### `onx_analytics` schema

The analytics schema intends to contain summary and metric tables for the purposes of reporting. These tables would be available to end users over a reporting platform such as Looker or Tableau.

The tables in this schema are organized in such a way as to make access to key metrics as quick as possible, having preaggregated data and comparisions to lower query runtimes.

Future tables in this schema may contain other levels of aggregations as more data becomes available, as well as history tables to keep track of slowly changing dimensions.

## Analysis

### Assumptions
Some assumptions were made to analyze the data provided. They were made to the best of my knowledge of the data, and are as follows:

1. Parcels without a geometry are invalid. They have been removed from the raw data.
2. Parcels without a `parcel_id` are invalid. They have been removed from the raw data.
3. Parcels with two or more geometry entires have their areas summed together when calculating land area.
4. Real property is defined as property with an account number starting with `R` - properties with other account numbers contain records for buildings that exist on parcels and are not counted when summing land area or counts of parcels by type.
5. When two or more owners each have a 100% ownership stake in a parcel, all are given full credit for the land area in the analysis as well as the `onx_analytics.owner_summary` table.
6. Residential parcels are defined as Real Account parcels having an `account_type` in the following list:
	- `APARTMENT`
	- `MIXED USE`
	- `RESIDENT LAND`
	- `RESIDENTIAL`
	- `RESIDENTIAL CONDO`
7. Analysis questions were answered using data from 2021-05-26, however the data set is updated daily, and changes to size and shape of parcels as well as ownership of those parcels occur on a daily basis.

### Data Issues
The following issues were identified in the data. Further research would be needed to know how to properly handle these cases.

- Parcels aren't always single units of land. They can have multiple geometries with the same parcel number.
- Some parcels have no associated geometry in the shapefile. 
- Some owners have records for a `parcel_id` that does not exist in the shapefile.
- Some parcels have no owner, and therefore no `account_type` to associate with the parcel.
- Multiple primary owners can exist for a single parcel with a total ownership percentage >100%. This often happens with accounts with the type `RESIDENTIAL CONDO` causing each owner to be counted as a whole owner of the land area comprising of the condo complex rather than just their individual condo.

### Analysis Questions

#### 1. How much total acreage of residential land exists in Boulder County?

107882.82 acres

~~~sql
with parcels as (
    select
        parcel_id,
        sum(st_area(geom, true)) as area_m2
    from onx_raw.parcels p
    where file_index = '20210526'
    group by 1
)

, parcel_account_type as (
    select distinct
        parcel_id
    from onx_raw.owners
    where is_real_property
        and account_type in ('APARTMENT', 'MIXED USE', 'RESIDENT LAND',
                             'RESIDENTIAL', 'RESIDENTIAL CONDO')
        and file_index = '20210526'
)

select 
	sum(area_m2 * 0.00024711) as total_residential_land_acres
from parcels p
join parcel_account_type pat on pat.parcel_id = p.parcel_id;
~~~

#### 2. Who owns the most acreage of residential lands?

ST VRAIN RESERVOIR & FISH CO with 245.94 acres of residential land area owned.

Query from raw data:

~~~sql
with parcels as (
    select
        parcel_id,
        sum(st_area(geom, true)) as area_m2
    from onx_raw.parcels p
    where file_index = '20210526'
    group by 1
)

,  ownership as (
    select distinct owner_name, parcel_id, coalesce(pct_own,1) pct_own
    from onx_raw.owners
    where is_real_property
        and account_type in ('APARTMENT', 'MIXED USE', 'RESIDENT LAND',
                             'RESIDENTIAL', 'RESIDENTIAL CONDO')
)

select 
	owner_name, 
	sum(area_m2 * o.pct_own * 0.00024711) as total_owned_residential
from parcels p
join ownership o on o.parcel_id = p.parcel_id
group by 1
order by 2 desc
~~~

Query from analytics summary table:

~~~sql
select 
	owner_name,
	total_residential_land_area_owned_acres
from onx_analytics.owner_daily_summary
where report_date = '2021-05-26'
order by 2 desc;
~~~

#### 3. Who owns the most total properties?

COUNTY OF BOULDER with 1291 real account properties.

Query from raw data:

~~~sql
select 
    owner_name, 
    count(distinct o.parcel_id) as total_properties
from onx_raw.owners o
join (
    select distinct parcel_id 
    from onx_raw.parcels
) p on p.parcel_id = o.parcel_id
where is_real_property
    and file_index = '20210526'
group by 1
order by 2 desc;
~~~

Query from analytics summary table:

~~~sql
select 
	owner_name,
	total_real_property_owned
from onx_analytics.owner_daily_summary
where report_date = '2021-05-26'
order by 2 desc;
~~~

## Future Developments/Improvements

There are many improvements to this pipeline that could be made, but did not fit in the scope of the project. This is built fairly specifically to the data that was provided, but could be generalized to handle different sources and more data, but following a similar patter of loading raw data from a source then building analytics tables to match the company's/end users needs/metrics. A short list of improvements is listed below:

- Genericise load of various files (csv, shapefile, etc) so that there is a single load job that can adapt to the needs of the file through the use of metadata.
- Automate retrieving new source data, preferably through an API and store data on a cloud storage system like s3 or Google Cloud Storage.
- Separate Load and Transform portions of the pipeline into separate scheduleable jobs.
- Make use of multithreading/multiprocessing to load and/or transform in parallel.
- Build out a larger schema of analytics tables based on the needs of the end user.
- Real-time job monitoring and logging to better catch errors, or re-run failed jobs.