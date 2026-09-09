//! Finite fixture inputs. No live MQTT/HTTP sources.

use sparrow_model::{
    DataType, DynamicValue, Field, FieldId, Result, Row, Scalar, Schema, SchemaId, SourceFrame,
};

/// Built-in sensor schema used by the M0 smoke demo and G1a suite.
pub fn sensor_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "temperature", DataType::Float64, true),
            Field::new(FieldId::new(3), "humidity", DataType::Float64, true),
            Field::new(FieldId::new(4), "ts", DataType::TimestampMicrosUTC, false),
            Field::new(FieldId::new(5), "payload", DataType::Dynamic, true),
        ],
    )
    .expect("sensor schema")
}

#[derive(Clone, Debug)]
pub struct SensorRecord {
    pub device_id: String,
    pub temperature: f64,
    pub humidity: f64,
    pub ts: i64,
    pub alert: bool,
}

impl SensorRecord {
    pub fn to_row(&self) -> Row {
        let payload = DynamicValue::object(vec![
            ("temp", DynamicValue::Float64(self.temperature)),
            ("humidity", DynamicValue::Float64(self.humidity)),
            ("alert", DynamicValue::Bool(self.alert)),
        ]);
        Row {
            values: vec![
                Scalar::utf8(&self.device_id),
                Scalar::Float64(self.temperature),
                Scalar::Float64(self.humidity),
                Scalar::TimestampMicrosUTC(self.ts),
                Scalar::Dynamic(payload),
            ],
        }
    }

    pub fn to_frame(&self) -> SourceFrame {
        let line = format!(
            "{},{},{},{},{}",
            self.device_id, self.temperature, self.humidity, self.ts, self.alert
        );
        SourceFrame::new(line.into_bytes(), self.ts)
    }
}

/// Finite, deterministic sensor stream used by tests and the smoke demo.
pub fn sensor_fixture() -> Vec<SensorRecord> {
    vec![
        SensorRecord {
            device_id: "edge-a".into(),
            temperature: 18.5,
            humidity: 41.0,
            ts: 1_700_000_000_000_000,
            alert: false,
        },
        SensorRecord {
            device_id: "edge-a".into(),
            temperature: 26.2,
            humidity: 39.0,
            ts: 1_700_000_001_000_000,
            alert: false,
        },
        SensorRecord {
            device_id: "edge-b".into(),
            temperature: 31.0,
            humidity: 55.0,
            ts: 1_700_000_002_000_000,
            alert: true,
        },
        SensorRecord {
            device_id: "edge-b".into(),
            temperature: 22.0,
            humidity: 48.0,
            ts: 1_700_000_003_000_000,
            alert: false,
        },
        SensorRecord {
            device_id: "edge-c".into(),
            temperature: 29.4,
            humidity: 33.0,
            ts: 1_700_000_004_000_000,
            alert: true,
        },
        SensorRecord {
            device_id: "edge-c".into(),
            temperature: 12.0,
            humidity: 70.0,
            ts: 1_700_000_005_000_000,
            alert: false,
        },
    ]
}

pub fn sensor_frames() -> Result<Vec<SourceFrame>> {
    Ok(sensor_fixture().into_iter().map(|r| r.to_frame()).collect())
}
