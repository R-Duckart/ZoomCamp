{% macro payment_type_lookup(payment_type) %}
    {%- set payment_relation = ref('payment_type_lookup') -%}
    case {{ payment_type }}
        {% if execute %}
            {% set results = run_query("select payment_type, description from " ~ payment_relation) %}
            {% for row in results %}
                when {{ row.payment_type }} then '{{ row.description }}'
            {% endfor %}
        {% endif %}
        else 'Unknown'
    end
{% endmacro %}