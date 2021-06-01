# dbt-works

Library of dbt challenges overcome

```bash
# setup python virtual environment locally
python -m venv py_venv
source py_venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

```bash
# set the profiles directory in an environment variable, so debug points to the right files
export DBT_PROFILES_DIR=$(pwd)

dbt debug

```
