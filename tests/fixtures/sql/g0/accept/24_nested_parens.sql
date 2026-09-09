-- Sparrow SQL v0 accept: nested boolean
SELECT * FROM sensor_readings WHERE (temperature > 20 AND humidity < 80) OR device_id = 'edge-c';
