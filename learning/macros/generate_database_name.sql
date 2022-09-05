{%- macro generate_database_name(custom_database_name=none, node=none) -%}
    {%- set default_database = target.database -%}

    {%- if custom_database_name is none -%}

        {{ default_database }}

    {%- elif target.name == 'SBX' -%}

        {{ target.name }}_{{ custom_database_name | trim }}

    {%- elif target.name == 'PPD' -%}

        {{ target.name }}_{{ custom_database_name | trim }}

    {%- elif target.name == 'PROD' -%}

        {{ custom_database_name | trim }}

    {%- else -%}

        {{ target.name }}_{{ custom_database_name | trim }}

    {%- endif -%}

{%- endmacro -%}

