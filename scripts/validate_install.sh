rm -rf virtualenv_elementary_test/
python3 -m venv virtualenv_elementary_test
source virtualenv_elementary_test/bin/activate
source virtualenv_elementary_test/bin/activate.fish
python3 -m ensurepip --upgrade
python3 -m pip install --upgrade pip
python3 -m pip install elementary-data
python3 -c "import monitor"
python3 -m pip install elementary-data[bigquery]
python3 -m pip install elementary-data[snowflake]
python3 -m pip install elementary-data[redshift]
python3 -m pip install elementary-data[postgres]
python3 -m pip install elementary-data[lineage]
python3 -c "import lineage"
deactivate