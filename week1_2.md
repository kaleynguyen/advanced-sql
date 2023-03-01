
```sql
Step 1: deduplicate the us cities
Step 2: Find eligible customers who have entered the address to get it delivered and answered the survey
Step 3: Find the customers who have at least one food preference with up to 3 food preference
Step 4: Transform the food preference column into 3 columns where each column represents the first, second and third preference
Step 5: Find **any** recipe that includes the recipe tags from the list of recipe
Step 6: Join the customer's data with suggested recipes from step 5, deduplicate the result and sort the result by email. 
*/
with 
    us_cities as (
        select
            city_name,
            state_abbr,
            geo_location
        from resources.us_cities
        qualify row_number() over (partition by city_name, state_abbr order by county_name) = 1
    )
    
    , eligible_customers as (
        select 
            customer_id,
            email,
            first_name,
            last_name 
        from vk_data.customers.customer_data as customers
        join vk_data.customers.customer_address as address using (customer_id)
        join us_cities as cities on upper(trim(address.customer_state)) = trim(cities.state_abbr)
             and upper(trim(address.customer_city)) = trim(cities.city_name)
    	join vk_data.customers.customer_survey as survey using (customer_id)
        where survey.is_active = true
    )
    
    , columnar_pref as (
    	select 
        	survey.customer_id,
            lower(trim(tags.tag_property)) as food_pref,
            dense_rank() over(partition by customer_id order by tag_property asc) as rnk
    	from vk_data.customers.customer_survey as survey
		join vk_data.resources.recipe_tags as tags using (tag_id)
        qualify dense_rank() over(partition by customer_id order by tag_property asc) <= 3
    )

    , rowise_pref as (
    	select *
        from columnar_pref
        	pivot(max(food_pref) for rnk in ('1','2','3'))
            as p (customer_id, food_pref_1, food_pref_2, food_pref_3)
    )
    
 	, recipe_with_tags as (
    	select 
         	trim(replace(flat_tag.value, '"', '')) as recipe_tags,
        	any_value(recipe_name) as suggested_recipe
        from vk_data.chefs.recipe
        	, table(flatten(tag_list)) as flat_tag
        group by 1
                
     )
     , final as (
    	select 
        	c.*,
            pref.food_pref_1,
            pref.food_pref_2,
            pref.food_pref_3,
            suggested_recipe
        from eligible_customers as c
        join rowise_pref as pref using (customer_id)
        join recipe_with_tags as rec on pref.food_pref_1 = rec.recipe_tags
    )
    
select distinct * 
from final
order by email;
```