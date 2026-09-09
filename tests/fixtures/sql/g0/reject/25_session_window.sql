-- Sparrow SQL v0 reject: SESSION window helper
SELECT SESSION(ts, INTERVAL '5' MINUTE) FROM sensor_readings;
