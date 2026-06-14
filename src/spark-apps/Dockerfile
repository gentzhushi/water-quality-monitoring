FROM apache/spark:3.5.5

USER root

RUN python3 -m pip install --no-cache-dir \
    joblib==1.4.2 \
    numpy==1.24.4 \
    pandas==2.0.3 \
    scikit-learn==1.3.2

USER spark
