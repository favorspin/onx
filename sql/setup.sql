create extension postgis;

create schema onx_raw;
create schema onx_analytics;

create table onx_raw.owners (
    account_id varchar,
    parcel_id varchar,
    lot varchar,
    is_real_property boolean,
    status_cd varchar,
    address varchar,
    address_unit varchar,
    city varchar,
    owner_name varchar,
    pct_own decimal(10,3),
    account_type varchar,
    file_index varchar,
    created_at timestamp default current_timestamp
);

create table onx_raw.parcels (
    object_id varchar,
    parcel_id varchar,
    shape_area varchar,
    shape_len varchar,
    geom geometry,
    file_index varchar
);

create table onx_analytics.owner_daily_summary (
    report_date date,
    owner_name varchar,
    total_real_property_owned int,
    total_building_property_owned int,
    total_real_property_land_area_owned_m2 decimal(38,10),
    total_real_property_land_area_owned_acres decimal(38,10),
    total_residential_parcels_owned int,
    total_residential_land_area_owned_m2 decimal(38,10),
    total_residential_land_area_owned_acres decimal(38,10),
    avg_real_property_investment_stake decimal(38,10),
    avg_building_property_investment_stake decimal(38,10),
    real_property_gain_loss int,
    pct_land_area_gain_loss decimal(38,10)
);

create table onx_analytics.land_type_daily_summary
(
    report_date date,
    account_type varchar,
    is_residential boolean,
    is_commercial boolean,
    total_parcels int,
    total_owners int,
    land_area_m2 decimal(38,10),
    land_area_acres decimal(38,10),
    pct_total_land_area decimal(38,10),
    total_parcel_gain_loss int,
    pct_land_area_gain_loss decimal(38,10)
);

