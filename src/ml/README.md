# Early-Warning ML Model

This folder contains an isolated supervised ML model for water-quality risk prediction.
The model is trained offline here, then loaded by the Spark streaming job for live inference.
The dashboard reads the live prediction tables through the Digital Twin backend.

The model answers:

```text
Given the last few minutes of readings, what is the risk that the location
will enter a warning or critical water-quality state in the next 10 minutes?
```

## Model Choice

The first model is a `RandomForestClassifier`.

It is a good fit for this demo because it:

- learns nonlinear relationships between parameters,
- trains quickly on synthetic data,
- gives feature importances for explanation,
- is easier to defend than a deep-learning model.

## Training Data

`generate_training_data.py` creates synthetic scenario runs for:

- `normal`
- `storm_runoff`
- `oxygen_depletion`
- `chemical_shift`
- `sensor_fault`
- `gradual_degradation`

Each feature row summarizes a recent five-minute window and labels what happens in the next ten minutes.
The training data uses the same environmental freshwater warning and critical rules that Spark seeds in Cassandra.
Those rules are reference-backed demo limits, not certified legal compliance limits.

## How To Run

The saved model should be generated with the same Python package versions that Spark uses at runtime.
From the repository root, use the Spark image:

```sh
docker run --rm -v "$PWD/src/ml:/opt/ml" -w /opt/ml water-quality-spark-ml:latest python3 generate_training_data.py
docker run --rm -v "$PWD/src/ml:/opt/ml" -w /opt/ml water-quality-spark-ml:latest python3 train_model.py
```

Generated files:

- `data/water_quality_training.csv`
- `artifacts/early_warning_random_forest.joblib`
- `artifacts/feature_schema.json`
- `artifacts/training_report.json`
- `artifacts/training_report.md`

## Outputs

The trained model returns:

- `risk_score`: probability-like score from `0.0` to `1.0`
- `risk_level`: `LOW`, `MEDIUM`, `HIGH`, or `CRITICAL`
- `predicted_event_type`: the most likely non-normal scenario

The current statistical anomaly score remains useful for "what is happening now."
This model is intended to add "what is likely to happen soon."

## Runtime Integration

Spark loads:

- `artifacts/early_warning_random_forest.joblib`
- `artifacts/feature_schema.json`

It builds five-minute feature windows from live readings and writes prediction results to Cassandra.
Low-risk predictions use `predicted_event_type=none` so the UI does not show a scary event label when the risk is low.
