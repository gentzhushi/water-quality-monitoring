# Early-Warning ML Training Report

- Trained at: `2026-06-14T19:00:45.783216+00:00`
- Model: `RandomForestClassifier`
- Version: `synthetic-rf-v1`
- Training rows: `56160`
- Test rows: `14040`
- Accuracy: `0.9424`
- Macro F1: `0.9407`

## Per-Class Metrics

| Class | Precision | Recall | F1 | Support |
| --- | ---: | ---: | ---: | ---: |
| chemical_shift | 0.9808 | 0.9537 | 0.9671 | 1231 |
| gradual_degradation | 0.9933 | 0.9401 | 0.9660 | 1253 |
| normal | 0.9074 | 0.9929 | 0.9482 | 7332 |
| oxygen_depletion | 0.9842 | 0.9973 | 0.9907 | 1125 |
| sensor_fault | 1.0000 | 0.7757 | 0.8737 | 1605 |
| storm_runoff | 0.9864 | 0.8246 | 0.8983 | 1494 |

## Top Feature Importances

| Feature | Importance |
| --- | ---: |
| turbidity_latest | 0.079282 |
| temperature_mean_5m | 0.073162 |
| conductivity_latest | 0.072472 |
| conductivity_mean_5m | 0.065708 |
| dissolved_oxygen_latest | 0.062161 |
| ph_mean_5m | 0.059706 |
| dissolved_oxygen_mean_5m | 0.058894 |
| turbidity_mean_5m | 0.055967 |
| ph_latest | 0.055824 |
| temperature_latest | 0.051569 |
| avg_threshold_distance | 0.040415 |
| max_threshold_distance | 0.036365 |
| turbidity_slope_5m | 0.033292 |
| orp_mean_5m | 0.031972 |
| turbidity_std_5m | 0.031424 |
| orp_latest | 0.026072 |
| critical_count_5m | 0.025954 |
| turbidity_threshold_distance | 0.025281 |
| ph_std_5m | 0.020364 |
| warning_count_5m | 0.016815 |

## Example Predictions

- `normal` sample from `normal_000` minute `5`: risk `0.3178`, level `LOW`, event `none`
- `storm_runoff` sample from `storm_runoff_000` minute `24`: risk `0.3201`, level `LOW`, event `none`
- `oxygen_depletion` sample from `oxygen_depletion_000` minute `45`: risk `0.9453`, level `CRITICAL`, event `oxygen_depletion`
- `chemical_shift` sample from `chemical_shift_000` minute `42`: risk `0.9524`, level `CRITICAL`, event `chemical_shift`
- `sensor_fault` sample from `sensor_fault_000` minute `25`: risk `0.3192`, level `LOW`, event `none`
- `gradual_degradation` sample from `gradual_degradation_000` minute `36`: risk `0.3204`, level `LOW`, event `none`
