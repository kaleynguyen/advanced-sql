```sql

with active_customers as (
	select 
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
),

chicago_il as (
	select 
        geo_location
    from vk_data.resources.us_cities 
    where state_abbr = 'IL' and city_name = 'CHICAGO'
),

gary_in as (
	select 
        geo_location
    from vk_data.resources.us_cities 
    where state_abbr = 'IN' and city_name = 'GARY'
)

select 
    first_name || ' ' || last_name as
customer_name,
    ca.customer_city,
    ca.customer_state,
    active_customers.food_pref_count,
    (st_distance(us.geo_location, chic.geo_location) / 1609)::int as chicago_distance_miles,
    (st_distance(us.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
from vk_data.customers.customer_address as ca
inner join vk_data.customers.customer_data using (customer_id)
left join vk_data.resources.us_cities as us 
	on upper(trim(ca.customer_state)) = upper(trim(us.state_abbr))
    and upper(trim(ca.customer_city)) = upper(trim(us.city_name))
inner join active_customers using (customer_id)
cross join chicago_il as chic
cross join gary_in as gary
where
	( trim(city_name) ilike any ('%concord%', '%georgetown%', '%ashland%') 
    					and customer_state = 'KY')
    or
    ( trim(city_name) ilike any ('%oakland%', '%pleasant hill%') 
    					and customer_state = 'CA' )
    or
    ( trim(city_name) ilike any ('%arlington%', '%brownsville%') 
    					and customer_state = 'TX');


```