/* 
    Validates that all geolocation coordinates fall within Brazil's bounding box. 

    Latitude: -33.0 to 5.0 
    Longitude: -73.0 to -35.0 

    Any rows returned by this test are considered failures.
*/

{{ config(severity='warn') }}

select 
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng
from {{ ref('stg_geolocation') }}
where 
    geolocation_lat < -33.0
    or geolocation_lat > 5.0
    or geolocation_lng < -73.0
    or geolocation_lng > -35.0