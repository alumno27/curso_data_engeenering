{% test relationships_to_dim_date(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and date_trunc('day', {{ column_name }}) not in (
      select date
      from {{ ref('dim_date') }}
  )
{% endtest %}
