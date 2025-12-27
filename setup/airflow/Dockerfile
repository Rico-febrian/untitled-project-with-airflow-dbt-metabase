FROM apache/airflow:2.10.2

COPY requirements.txt dbt-requirements.txt ./

# Install requirements biasa
RUN pip install --no-cache-dir -r requirements.txt

# Install cosmos + dbt stack dengan versi exact - NO BACKTRACKING!
RUN pip install --no-cache-dir \
    astronomer-cosmos==1.12.0 \
    dbt-core==1.8.7 \
    dbt-postgres==1.9.0 \
    dbt-adapters==1.9.0 \
    dbt-common==1.12.0 \
    dbt-extractor==0.6.0 \
    dbt-semantic-interfaces==0.5.1

# dbt venv
RUN python -m virtualenv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && \
    deactivate

COPY ./variables-and-connections /init/variables-and-connections