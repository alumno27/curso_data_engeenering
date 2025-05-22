{% test unique_combination_of_columns(model, combination_of_columns) %}
select
  {{ combination_of_columns | join(', ') }},
  count(*) as count_duplicates
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count_duplicates > 1
{% endtest %}
