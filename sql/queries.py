# owner data queries
owner_create_temp_table = """
            DROP TABLE IF EXISTS onx_raw.temp_owners;
            CREATE TABLE onx_raw.temp_owners (
                created_date timestamp,
                strap varchar,
                folio varchar,
                status_cd varchar,
                building_num varchar,
                street_num varchar,
                street varchar,
                street_prefix varchar,
                street_suffix varchar,
                street_unit varchar,
                city varchar,
                sub_code varchar,
                sub_description varchar,
                section varchar,
                township varchar,
                range varchar,
                block varchar,
                lot varchar,
                owner_name varchar,
                mail_to varchar,
                mailingAddr1 varchar,
                mailingAddr2 varchar,
                mailingCity varchar,
                mailingState varchar,
                mailingZip varchar,
                mailingCountry varchar,
                role_cd varchar,
                pct_own varchar,
                taxArea varchar,
                nh varchar,
                mill_levy varchar,
                legalDscr varchar,
                waterFee varchar,
                account_type varchar
            );
        """
owner_check_data = "SELECT * FROM onx_raw.owners WHERE file_index = '{}' LIMIT 1"

owner_drop_temp_table = "DROP Table onx_raw.temp_owners;"

owner_copy_data = """
            COPY onx_raw.temp_owners
            FROM STDIN WITH CSV HEADER DELIMITER AS ',' NULL AS '';
        """

owner_add_file_index = """
            ALTER TABLE onx_raw.temp_owners
            ADD COLUMN file_index varchar;
            UPDATE onx_raw.temp_owners set file_index = '{}';
        """

owner_clean_raw_table = """
            DELETE FROM onx_raw.owners
            WHERE file_index = '{}';
        """

owner_load_raw_table = """
            INSERT INTO onx_raw.owners
                (account_id, parcel_id, lot, is_real_property, status_cd, address,
                 address_unit, city, owner_name, pct_own, account_type, file_index)
            SELECT DISTINCT
                strap as account_id,
                folio as parcel_id,
                lot,
                case when strap like 'R%' then true else false end as is_real_property,
                status_cd,
                coalesce(street_num, '') || ' ' ||
                    case when street_prefix is null then '' else street_prefix || ' ' end ||
                    coalesce(street, '') || ' ' || coalesce(street_suffix, '') as address,
                street_unit as address_unit,
                city,
                owner_name,
                pct_own::decimal(10,3),
                account_type,
                file_index
            FROM onx_raw.temp_owners;
        """


# parcel data queries

parcel_check_data = "SELECT * FROM onx_raw.parcels WHERE file_index = '{}' LIMIT 1"

parcel_create_temp_table = """
            DROP TABLE IF EXISTS onx_raw.temp_parcels;
            CREATE TABLE onx_raw.temp_parcels (
                object_id varchar,
                parcel_no varchar,
                shape_area varchar,
                shape_len varchar,
                geom geometry,
                file_index varchar
            );
        """

parcel_load_data = """
            INSERT INTO onx_raw.temp_parcels
                (object_id, parcel_no, shape_area, shape_len, geom, file_index)
            values
                ('{0}', '{1}', '{2}', '{3}', st_geomfromtext('{4}',4326), '{5}')
        """

parcel_clean_raw_table = """
            DELETE FROM onx_raw.parcels
            WHERE file_index = '{}';
        """

parcel_load_raw_table = """
            INSERT INTO onx_raw.parcels
            SELECT
                object_id,
                parcel_no as parcel_id,
                shape_area,
                shape_len,
                geom,
                file_index
            FROM onx_raw.temp_parcels
            WHERE parcel_no <> '';
        """

parcel_drop_temp_table = "DROP Table onx_raw.temp_parcels;"



# analytic table queries

analytic_rebuild = """
            TRUNCATE TABLE onx_analytics.owner_daily_summary;
            INSERT INTO onx_analytics.owner_daily_summary
            WITH ownership as (
                select distinct owner_name, parcel_id, is_real_property, pct_own, account_type, file_index
                from onx_raw.owners
            )

            , parcels as (
                select
                    parcel_id,
                    file_index,
                    sum(st_area(geom, true)) as area_m2
                from onx_raw.parcels p
                group by 1,2
            )

            , daily_summary as (
                select
                    to_date(o.file_index, 'yyyyMMdd') as report_date,
                    o.owner_name,
                    count(distinct case when o.is_real_property then o.parcel_id end) as total_real_property_owned,
                    count(distinct case when not o.is_real_property then o.parcel_id end) as total_building_property_owned,
                    sum(case when o.is_real_property then p.area_m2 * o.pct_own end) as total_real_property_land_area_owned_m2,
                    sum(case when o.is_real_property then p.area_m2 * o.pct_own * 0.00024711 end) as total_real_property_land_area_owned_acres,
                    count(distinct case when o.is_real_property and o.account_type in (
                            'APARTMENT', 'MIXED USE', 'RESIDENT LAND', 'RESIDENTIAL', 'RESIDENTIAL CONDO')
                            then o.parcel_id end) as total_residential_parcels_owned,
                    coalesce(sum(case when o.is_real_property and o.account_type in (
                            'APARTMENT', 'MIXED USE', 'RESIDENT LAND', 'RESIDENTIAL', 'RESIDENTIAL CONDO')
                            then p.area_m2 * o.pct_own end),0) as total_residential_land_area_owned_m2,
                    coalesce(sum(case when o.is_real_property and o.account_type in (
                            'APARTMENT', 'MIXED USE', 'RESIDENT LAND', 'RESIDENTIAL', 'RESIDENTIAL CONDO')
                            then p.area_m2 * o.pct_own * 0.00024711 end),0) as total_residential_land_area_owned_acres,
                    avg(case when o.is_real_property then o.pct_own end) as avg_real_property_investment_stake,
                    coalesce(avg(case when not o.is_real_property then o.pct_own end),0) as avg_building_property_investment_stake
                from ownership o
                join parcels p on p.parcel_id = o.parcel_id
                    and p.file_index = o.file_index
                group by 1,2
            )

            select *,
                total_real_property_owned
                    - coalesce(lag(total_real_property_owned, 1) over (partition by owner_name order by report_date asc) ,0) as real_property_gain_loss,
                1 - (coalesce(lag(total_real_property_land_area_owned_m2, 1) over (partition by owner_name order by report_date asc),0)
                    / nullif(total_real_property_land_area_owned_m2,0)) as pct_land_area_gain_loss
            from daily_summary
            order by report_date desc
        """