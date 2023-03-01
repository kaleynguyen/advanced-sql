```sql
\*
Step 1: deduplicate city_name and state_abbr to return a us_cities cte
Step 2: us_cities join with customer data to get the customer's geolocation
Step 3: us_cities join with supplier info to get the supplier's geo location 
Step 4: cross join of customer and supplier to get the pairwise distance from their associated geolocation.
*\
with 
    us_cities as (
        select
            city_name,
            state_abbr,
            geo_location
        from resources.us_cities
        qualify row_number() over (partition by city_name, state_abbr order by county_name) = 1
    )
    , customers as (
    	select 
        	customer_data.*,
            us_cities.geo_location
        from customer_data
        join customer_address using (customer_id)
        join us_cities on upper(trim(customer_state)) = trim(state_abbr)
        	 and upper(trim(customer_city)) = trim(us_cities.city_name)
    )
    , suppliers as (
    	select supplier_info.*,
        	   us_cities.geo_location
        from suppliers.supplier_info
        join us_cities on upper(trim(supplier_info.supplier_city)) = trim(us_cities.city_name)
        	 and upper(trim(supplier_info.supplier_state)) = trim(us_cities.state_abbr)
    )
    , 	final as (
    	select 
        	customers.customer_id,
            customers.first_name,
            customers.last_name,
            customers.email,
            suppliers.supplier_id,
            suppliers.supplier_name,
            st_distance(customers.geo_location, suppliers.geo_location)::numeric / 1609	  	as distance_in_miles
	  	from customers
	  	cross join suppliers
		qualify row_number() over (partition by
 customers.customer_id order by distance_in_miles asc) = 1
		order by 3,2 )
select * from final;
```