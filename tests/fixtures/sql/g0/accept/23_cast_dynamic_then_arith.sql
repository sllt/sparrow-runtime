-- Sparrow SQL v0 accept: CAST clears Dynamic restriction
SELECT CAST(payload AS DOUBLE) + 1 FROM sensor_readings;
