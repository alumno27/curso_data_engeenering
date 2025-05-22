select
    cast(USER_ID as string)   as user_id,
    upper(SEX)                as sex,
    try_cast(BIRTHDATE as date) as birthdate
from ALUMNO27_DEV_BRONZE_DB.ZIPCODE_DATA.users_data
