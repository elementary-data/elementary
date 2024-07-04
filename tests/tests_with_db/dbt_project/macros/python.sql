{% macro python_mock_test(args) %}
def test(model_df, ref, session):
  return {{ args.result }}
{% endmacro %}

{% macro python_return_df(args) %}
def test(model_df, ref, session):
  return model_df
{% endmacro %}

{% macro python_return_empty_df(args) %}
def test(model_df, ref, session):
  col_name = model_df.columns[0]
  col = model_df[col_name]
  return model_df[col == "blablablabla"]
{% endmacro %}