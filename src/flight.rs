//! Bulk interchange over Arrow Flight, on the API listener and behind the
//! same authentication: the namespace comes from the caller's identity.
//!
//! - `DoPut` upserts each Arrow batch of entities and answers it with one
//!   `PutResult` whose `app_metadata` is JSON outcome counts
//!   (`inserted`, `updated`, `stale`, `refused`, and the first refusal).
//! - `DoGet` takes a ticket holding a JSON `QueryRequest` (the HTTP query
//!   body) and streams every entity it selects, page by page. Pages are read
//!   one after another, so writes landing meanwhile may or may not appear. A
//!   nearest query exports its `k` entities in one page, so `k` is capped.
//!
//! Both use [`schema`]: geometry is GeoArrow WKB (`geoarrow.wkb`, GeoArrow
//! spec 0.2, CRS `OGC:CRS84`), so Arrow query engines read exports directly.
//! Fields are a JSON object: integers are written without a fraction and
//! floats with one, so `Int(1)` and `Float(1.0)` survive a round trip.
//! Labels are a JSON array of strings. A ticket's `label_filter` limits the
//! export as it limits a query, and governed mode requires one.

use crate::api::{Api, ApiError, metadata_auth, proto, query_request};
use crate::codec::{geometry_from_wkb, geometry_to_wkb};
use crate::entity::{Entity, FieldValue, Labels};
use crate::index::Upsert;
use crate::query::MAX_LIMIT;
use arrow_array::builder::{BinaryBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Float64Array, Int64Array, LargeBinaryArray, RecordBatch,
    StringArray,
};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use futures::{Stream, StreamExt, TryStreamExt};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;

/// The GeoArrow extension the geometry column carries.
pub const GEOARROW_EXTENSION: &str = "geoarrow.wkb";
/// Its metadata: coordinates are WGS 84 longitude and latitude.
pub const GEOARROW_METADATA: &str = r#"{"crs":"OGC:CRS84"}"#;

/// The batch schema of both directions.
pub fn schema() -> Schema {
    let geometry = Field::new("geometry", DataType::Binary, false).with_metadata(HashMap::from([
        (
            "ARROW:extension:name".to_string(),
            GEOARROW_EXTENSION.to_string(),
        ),
        (
            "ARROW:extension:metadata".to_string(),
            GEOARROW_METADATA.to_string(),
        ),
    ]));
    Schema::new(vec![
        Field::new("collection", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, false),
        geometry,
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("speed", DataType::Float64, true),
        Field::new("heading", DataType::Float64, true),
        Field::new("ttl_ms", DataType::Int64, true),
        Field::new("fields", DataType::Utf8, false),
        Field::new("labels", DataType::Utf8, false),
    ])
}

fn fields_json(fields: &BTreeMap<String, FieldValue>) -> String {
    let object: serde_json::Map<String, serde_json::Value> = fields
        .iter()
        .map(|(name, value)| {
            let value = match value {
                FieldValue::Bool(v) => serde_json::Value::Bool(*v),
                FieldValue::Int(v) => serde_json::Value::from(*v),
                // Fields are finite, so this never falls back to null.
                FieldValue::Float(v) => serde_json::Number::from_f64(*v)
                    .map_or(serde_json::Value::Null, serde_json::Value::Number),
                FieldValue::Text(v) => serde_json::Value::String(v.clone()),
            };
            (name.clone(), value)
        })
        .collect();
    serde_json::Value::Object(object).to_string()
}

fn fields_from_json(text: &str) -> Result<BTreeMap<String, FieldValue>, String> {
    let object: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(text).map_err(|error| format!("fields: {error}"))?;
    object
        .into_iter()
        .map(|(name, value)| {
            let value = match value {
                serde_json::Value::Bool(v) => FieldValue::Bool(v),
                serde_json::Value::String(v) => FieldValue::Text(v),
                serde_json::Value::Number(n) => match n.as_i64() {
                    Some(v) => FieldValue::Int(v),
                    None => FieldValue::Float(
                        n.as_f64()
                            .ok_or_else(|| format!("field {name}: not a number"))?,
                    ),
                },
                _ => return Err(format!("field {name}: not a bool, number or string")),
            };
            Ok((name, value))
        })
        .collect()
}

/// Entities as one batch of [`schema`].
pub fn to_batch(entities: &[Entity]) -> Result<RecordBatch, ArrowError> {
    let mut collection = StringBuilder::new();
    let mut id = StringBuilder::new();
    let mut geometry = BinaryBuilder::new();
    let mut ts = Int64Builder::new();
    let mut speed = Float64Builder::new();
    let mut heading = Float64Builder::new();
    let mut ttl = Int64Builder::new();
    let mut fields = StringBuilder::new();
    let mut labels = StringBuilder::new();
    for entity in entities {
        collection.append_value(&entity.collection);
        id.append_value(&entity.id);
        geometry.append_value(geometry_to_wkb(&entity.geometry));
        ts.append_value(entity.ts_ms);
        speed.append_option(entity.speed);
        heading.append_option(entity.heading);
        ttl.append_option(entity.ttl_ms);
        fields.append_value(fields_json(&entity.fields));
        labels
            .append_value(serde_json::Value::from_iter(entity.labels.iter().cloned()).to_string());
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(collection.finish()),
        Arc::new(id.finish()),
        Arc::new(geometry.finish()),
        Arc::new(ts.finish()),
        Arc::new(speed.finish()),
        Arc::new(heading.finish()),
        Arc::new(ttl.finish()),
        Arc::new(fields.finish()),
        Arc::new(labels.finish()),
    ];
    RecordBatch::try_new(Arc::new(schema()), columns)
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<Option<&'a T>, String> {
    match batch.column_by_name(name) {
        None => Ok(None),
        Some(array) => array
            .as_any()
            .downcast_ref::<T>()
            .map(Some)
            .ok_or_else(|| format!("column {name} has type {}", array.data_type())),
    }
}

fn required<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, String> {
    column(batch, name)?.ok_or_else(|| format!("column {name} is required"))
}

/// The rows of a batch as entities, each converted on its own: a row that
/// does not convert is refused alone. Columns are found by name; `speed`,
/// `heading`, `ttl_ms`, `fields` and `labels` may be absent, and geometry
/// may be `Binary` or `LargeBinary` WKB.
pub fn from_batch(batch: &RecordBatch) -> Result<Vec<Result<Entity, String>>, String> {
    let collection: &StringArray = required(batch, "collection")?;
    let id: &StringArray = required(batch, "id")?;
    let ts: &Int64Array = required(batch, "ts_ms")?;
    let speed: Option<&Float64Array> = column(batch, "speed")?;
    let heading: Option<&Float64Array> = column(batch, "heading")?;
    let ttl: Option<&Int64Array> = column(batch, "ttl_ms")?;
    let fields: Option<&StringArray> = column(batch, "fields")?;
    let labels: Option<&StringArray> = column(batch, "labels")?;
    let narrow: Option<&BinaryArray> = column(batch, "geometry").unwrap_or(None);
    let wide: Option<&LargeBinaryArray> = column(batch, "geometry").unwrap_or(None);
    if narrow.is_none() && wide.is_none() {
        return Err("column geometry (WKB, Binary or LargeBinary) is required".into());
    }
    let optional_f64 = |array: Option<&Float64Array>, row: usize| {
        array.and_then(|a| (!a.is_null(row)).then(|| a.value(row)))
    };
    Ok((0..batch.num_rows())
        .map(|row| {
            if collection.is_null(row) || id.is_null(row) || ts.is_null(row) {
                return Err("collection, id and ts_ms must not be null".to_string());
            }
            let wkb = match (narrow, wide) {
                (Some(a), _) if !a.is_null(row) => a.value(row),
                (_, Some(a)) if !a.is_null(row) => a.value(row),
                _ => return Err("geometry must not be null".to_string()),
            };
            Ok(Entity {
                collection: collection.value(row).to_string(),
                id: id.value(row).to_string(),
                geometry: geometry_from_wkb(wkb).map_err(|error| error.to_string())?,
                ts_ms: ts.value(row),
                speed: optional_f64(speed, row),
                heading: optional_f64(heading, row),
                ttl_ms: ttl.and_then(|a| (!a.is_null(row)).then(|| a.value(row))),
                fields: match fields {
                    Some(a) if !a.is_null(row) => fields_from_json(a.value(row))?,
                    _ => BTreeMap::new(),
                },
                labels: match labels {
                    Some(a) if !a.is_null(row) => serde_json::from_str(a.value(row))
                        .map_err(|error| format!("labels: {error}"))?,
                    _ => Labels::new(),
                },
            })
        })
        .collect())
}

/// Outcome counts of one put batch.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PutCounts {
    pub inserted: u64,
    pub updated: u64,
    pub stale: u64,
    pub refused: u64,
    /// Why the first refused row was refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_refusal: Option<String>,
}

impl PutCounts {
    fn refuse(&mut self, why: String) {
        self.refused += 1;
        self.first_refusal.get_or_insert(why);
    }
}

type Boxed<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send>>;

fn unimplemented<T>(what: &str) -> Result<T, tonic::Status> {
    Err(tonic::Status::unimplemented(format!(
        "{what} is not offered; use DoPut and DoGet"
    )))
}

fn flight_status(error: FlightError) -> tonic::Status {
    match error {
        FlightError::Tonic(status) => *status,
        other => tonic::Status::invalid_argument(other.to_string()),
    }
}

#[tonic::async_trait]
impl FlightService for Api {
    type HandshakeStream = Boxed<HandshakeResponse>;
    type ListFlightsStream = Boxed<FlightInfo>;
    type DoGetStream = Boxed<FlightData>;
    type DoPutStream = Boxed<PutResult>;
    type DoExchangeStream = Boxed<FlightData>;
    type DoActionStream = Boxed<arrow_flight::Result>;
    type ListActionsStream = Boxed<ActionType>;

    async fn do_put(
        &self,
        request: tonic::Request<tonic::Streaming<FlightData>>,
    ) -> Result<tonic::Response<Self::DoPutStream>, tonic::Status> {
        let ns = self.namespace(metadata_auth(&request).as_deref())?;
        let mut batches = FlightRecordBatchStream::new_from_flight_data(
            request
                .into_inner()
                .map_err(|status| FlightError::Tonic(Box::new(status))),
        );
        let api = self.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(batch) = batches.next().await {
                let rows = match batch
                    .map_err(flight_status)
                    .and_then(|batch| from_batch(&batch).map_err(tonic::Status::invalid_argument))
                {
                    Ok(rows) => rows,
                    Err(status) => {
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                };
                let mut counts = PutCounts::default();
                let mut valid = Vec::new();
                for row in rows {
                    match row {
                        Ok(entity) => valid.push(entity),
                        Err(why) => counts.refuse(why),
                    }
                }
                let ns = ns.clone();
                let outcomes = api
                    .blocking(move |engine| {
                        Ok(engine.upsert_batch(&ns, valid, crate::api::now_ms())?)
                    })
                    .await;
                let outcomes = match outcomes {
                    Ok(outcomes) => outcomes,
                    Err(error) => {
                        let _ = sender.send(Err(error.into())).await;
                        return;
                    }
                };
                for outcome in outcomes {
                    match outcome {
                        Ok(Upsert::Inserted) => counts.inserted += 1,
                        Ok(Upsert::Updated) => counts.updated += 1,
                        Ok(Upsert::Stale) => counts.stale += 1,
                        Err(error) => counts.refuse(error.to_string()),
                    }
                }
                let result = PutResult {
                    app_metadata: serde_json::to_vec(&counts).unwrap_or_default().into(),
                };
                if sender.send(Ok(result)).await.is_err() {
                    return;
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
    }

    async fn do_get(
        &self,
        request: tonic::Request<Ticket>,
    ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
        let ns = self.namespace(metadata_auth(&request).as_deref())?;
        let ticket: proto::QueryRequest = serde_json::from_slice(&request.into_inner().ticket)
            .map_err(|error| tonic::Status::invalid_argument(format!("ticket: {error}")))?;
        let readable = self.readable(ticket.label_filter.clone())?;
        let mut query = query_request(ticket).map_err(tonic::Status::from)?;
        query.readable = readable;
        match query.query {
            // A nearest query is one page of `k`; a larger `k` would end
            // the export early, so it is refused instead.
            crate::query::Query::Nearest { k, .. } if k > MAX_LIMIT => {
                return Err(tonic::Status::invalid_argument(format!(
                    "a nearest export holds at most {MAX_LIMIT} entities"
                )));
            }
            crate::query::Query::Nearest { k, .. } => query.limit = k,
            _ if query.limit == 0 => query.limit = MAX_LIMIT,
            _ => {}
        }
        // The first page is read before the stream starts, so a bad query
        // fails the call itself.
        let api = self.clone();
        let read = move |query: crate::query::QueryRequest| {
            let (api, ns) = (api.clone(), ns.clone());
            async move {
                api.blocking(move |engine| Ok(engine.query(&ns, &query, crate::api::now_ms())?))
                    .await
            }
        };
        let first = read(query.clone()).await?;
        let pages = futures::stream::try_unfold((Some(first), query), move |(page, mut query)| {
            let read = read.clone();
            async move {
                let Some(page) = page else {
                    return Ok::<_, ApiError>(None);
                };
                let next = match page.next_cursor {
                    Some(cursor) => {
                        query.cursor = Some(cursor);
                        Some(read(query.clone()).await?)
                    }
                    None => None,
                };
                Ok(Some((page.entities, (next, query))))
            }
        })
        .map_err(|error| FlightError::Tonic(Box::new(error.into())))
        .and_then(|entities| async move { Ok(to_batch(&entities)?) });
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(Arc::new(schema()))
            .build(pages)
            .map_err(flight_status);
        Ok(tonic::Response::new(Box::pin(encoded)))
    }

    async fn handshake(
        &self,
        _request: tonic::Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<tonic::Response<Self::HandshakeStream>, tonic::Status> {
        unimplemented("Handshake (authenticate with a bearer token instead)")
    }

    async fn list_flights(
        &self,
        _request: tonic::Request<Criteria>,
    ) -> Result<tonic::Response<Self::ListFlightsStream>, tonic::Status> {
        unimplemented("ListFlights")
    }

    async fn get_flight_info(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> Result<tonic::Response<FlightInfo>, tonic::Status> {
        unimplemented("GetFlightInfo")
    }

    async fn poll_flight_info(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> Result<tonic::Response<PollInfo>, tonic::Status> {
        unimplemented("PollFlightInfo")
    }

    async fn get_schema(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> Result<tonic::Response<SchemaResult>, tonic::Status> {
        unimplemented("GetSchema")
    }

    async fn do_exchange(
        &self,
        _request: tonic::Request<tonic::Streaming<FlightData>>,
    ) -> Result<tonic::Response<Self::DoExchangeStream>, tonic::Status> {
        unimplemented("DoExchange")
    }

    async fn do_action(
        &self,
        _request: tonic::Request<Action>,
    ) -> Result<tonic::Response<Self::DoActionStream>, tonic::Status> {
        unimplemented("DoAction")
    }

    async fn list_actions(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Self::ListActionsStream>, tonic::Status> {
        unimplemented("ListActions")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{Geometry, Position};

    #[test]
    fn entities_round_trip_through_a_batch() {
        let entities = vec![
            Entity {
                collection: "c".into(),
                id: "a".into(),
                geometry: Geometry::Point(Position::new(1.5, -2.0)),
                ts_ms: 7,
                speed: Some(3.0),
                heading: None,
                ttl_ms: Some(1_000),
                fields: BTreeMap::from([
                    ("i".into(), FieldValue::Int(1)),
                    ("f".into(), FieldValue::Float(1.0)),
                    ("b".into(), FieldValue::Bool(true)),
                    ("t".into(), FieldValue::Text("x\"y".into())),
                ]),
                labels: ["eu".to_string(), "secret".to_string()].into(),
            },
            Entity {
                collection: "c".into(),
                id: "b".into(),
                geometry: Geometry::Line(vec![Position::new(0.0, 0.0), Position::new(1.0, 1.0)]),
                ts_ms: 8,
                speed: None,
                heading: Some(90.0),
                ttl_ms: None,
                fields: BTreeMap::new(),
                labels: Default::default(),
            },
        ];
        let batch = to_batch(&entities).unwrap();
        let back: Vec<Entity> = from_batch(&batch)
            .unwrap()
            .into_iter()
            .map(Result::unwrap)
            .collect();
        assert_eq!(back, entities);
        let geometry = batch.schema().field_with_name("geometry").unwrap().clone();
        assert_eq!(
            geometry
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some(GEOARROW_EXTENSION)
        );
    }

    #[test]
    fn a_bad_row_is_refused_alone_and_a_missing_column_refuses_the_batch() {
        let mut entities = vec![Entity {
            collection: "c".into(),
            id: "a".into(),
            geometry: Geometry::Point(Position::new(0.0, 0.0)),
            ts_ms: 1,
            speed: None,
            heading: None,
            ttl_ms: None,
            fields: BTreeMap::new(),
            labels: Default::default(),
        }];
        entities.push(Entity {
            id: "b".into(),
            ..entities[0].clone()
        });
        let batch = to_batch(&entities).unwrap();
        // Damage the second row's fields.
        let mut columns = batch.columns().to_vec();
        columns[7] = Arc::new(StringArray::from(vec!["{}", "[1]"]));
        let damaged = RecordBatch::try_new(batch.schema(), columns).unwrap();
        let rows = from_batch(&damaged).unwrap();
        assert!(rows[0].is_ok() && rows[1].is_err());
        let without_id = batch.project(&[0, 2, 3]).unwrap();
        assert!(from_batch(&without_id).is_err());
    }
}
