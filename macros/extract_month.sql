{% macro extract_month(months) %}
  case {{ months }}
    when  like "%january%" then 'january'
    when  like "%february%" then 'february'
    when  like "%march%" then 'march'
    when  like "%april%" then 'april'
    when  like "%may%" then 'may'
    when  like "%june%" then 'june'
    when  like "%july%" then 'july'
    when  like "%august%" then 'august'
    when  like "%september%" then 'september'
    when  like "%october%" then 'october'
    when  like "%november%" then 'november'
    when  like "%december%" then 'december'
    else null
  end
{% endmacro %}
