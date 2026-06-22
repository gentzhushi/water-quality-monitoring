# Early-Warning ML Training Report

- Trained at: `2026-06-22T20:44:49.212352+00:00`
- Model: `RandomForestClassifier`
- Version: `synthetic-rf-v1`
- Training rows: `28800`
- Test rows: `7200`
- Accuracy: `0.8519`
- Macro F1: `0.8561`

## Per-Class Metrics

| Class | Precision | Recall | F1 | Support |
| --- | ---: | ---: | ---: | ---: |
| chemical_shift | 1.0000 | 0.7761 | 0.8740 | 880 |
| gradual_degradation | 1.0000 | 0.7834 | 0.8786 | 882 |
| normal | 0.7284 | 0.9986 | 0.8424 | 2852 |
| oxygen_depletion | 0.9941 | 0.7752 | 0.8711 | 863 |
| sensor_fault | 1.0000 | 0.6432 | 0.7829 | 852 |
| storm_runoff | 1.0000 | 0.7979 | 0.8876 | 871 |

## Top Feature Importances

| Feature | Importance |
| --- | ---: |
| avg_threshold_distance | 0.090891 |
| turbidity_latest | 0.082794 |
| max_threshold_distance | 0.072345 |
| warning_count_5m | 0.067353 |
| turbidity_mean_5m | 0.063717 |
| conductivity_latest | 0.062518 |
| conductivity_mean_5m | 0.061055 |
| dissolved_oxygen_latest | 0.057867 |
| turbidity_threshold_distance | 0.051182 |
| dissolved_oxygen_mean_5m | 0.043472 |
| orp_mean_5m | 0.038638 |
| critical_count_5m | 0.037941 |
| temperature_mean_5m | 0.037654 |
| temperature_latest | 0.035812 |
| conductivity_threshold_distance | 0.028294 |
| orp_latest | 0.025010 |
| orp_threshold_distance | 0.017658 |
| turbidity_slope_5m | 0.017155 |
| ph_latest | 0.013499 |
| ph_threshold_distance | 0.013290 |

## Example Predictions

- `normal` sample from `normal_000` minute `5`: risk `0.274`, level `LOW`, event `none`
- `storm_runoff` sample from `storm_runoff_000` minute `30`: risk `0.2826`, level `LOW`, event `none`
- `oxygen_depletion` sample from `oxygen_depletion_000` minute `20`: risk `0.2634`, level `LOW`, event `none`
- `chemical_shift` sample from `chemical_shift_000` minute `25`: risk `0.271`, level `LOW`, event `none`
- `sensor_fault` sample from `sensor_fault_000` minute `26`: risk `0.2728`, level `LOW`, event `none`
- `gradual_degradation` sample from `gradual_degradation_000` minute `26`: risk `0.2775`, level `LOW`, event `none`
