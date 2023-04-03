 {#
    This macro extract the january the record was reported
#}

{% macro extract_month(months) %}
  case 
    when {{ months }} like "%january%" then 'january'
    when {{ months }} like "%february%" then 'february'
    when {{ months }} like "%march%" then 'march'
    when {{ months }} like "%april%" then 'april'
    when {{ months }} like "%may%" then 'may'
    when {{ months }} like "%june%" then 'june'
    when {{ months }} like "%july%" then 'july'
    when {{ months }} like "%august%" then 'august'
    when {{ months }} like "%september%" then 'september'
    when {{ months }} like "%october%" then 'october'
    when {{ months }} like "%november%" then 'november'
    when {{ months }} like "%december%" then 'december'
    else null
  end
{% endmacro %}
