//! The network API: gRPC (primary) and HTTP/JSON (secondary) over one
//! listener, both answering through the same core. Every request is
//! authenticated by the configured [`Authenticator`]; the namespace comes
//! from the resulting identity and from nowhere else.
//!
//! Arrow Flight bulk interchange (`DoPut`, `DoGet`) is served on the same
//! listener; see [`crate::flight`].
//!
//! HTTP routes (JSON bodies are the protobuf messages in snake_case, oneofs
//! as `{"variant": {...}}`):
//! `POST /v1/entities`, `GET|DELETE /v1/entities/{collection}/{id}`,
//! `POST /v1/query`, `GET /v1/status`, `GET /v1/fences`,
//! `PUT|GET|DELETE /v1/fences/{id}` and `GET /v1/events`.

use crate::auth::{AuthError, Authenticator, bearer};
use crate::durable::{DurableEngine, DurableError};
use crate::entity::{Entity, FieldValue, Namespace};
use crate::fence::{EventKind, Fence, FenceEvent, Shape};
use crate::geometry::{Geometry, Position};
use crate::index::{IndexError, Upsert};
use crate::query::{Filter, MAX_LIMIT, Query, QueryRequest, Relation};
use axum::Json;
use axum::extract::{Path, Query as Params, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Generated from `proto/ezu/v1/ezu.proto`.
pub mod proto {
    tonic::include_proto!("ezu.v1");
}

use proto::ezu_server::{Ezu, EzuServer};

/// A refused request: a gRPC code and the same meaning over HTTP.
#[derive(Debug)]
pub struct ApiError {
    code: tonic::Code,
    message: String,
}

impl ApiError {
    fn new(code: tonic::Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new(tonic::Code::InvalidArgument, message)
    }
}

impl From<AuthError> for ApiError {
    fn from(error: AuthError) -> Self {
        Self::new(tonic::Code::Unauthenticated, error.to_string())
    }
}

impl From<IndexError> for ApiError {
    fn from(error: IndexError) -> Self {
        let code = match error {
            IndexError::Invalid(_)
            | IndexError::InvalidWindow
            | IndexError::InvalidQuery(_)
            | IndexError::PartitionNotEmpty => tonic::Code::InvalidArgument,
            IndexError::EntityQuota { .. }
            | IndexError::CollectionQuota { .. }
            | IndexError::VertexQuota { .. }
            | IndexError::RateQuota { .. }
            | IndexError::FenceQuota { .. } => tonic::Code::ResourceExhausted,
            IndexError::Poisoned => tonic::Code::Unavailable,
        };
        Self::new(code, error.to_string())
    }
}

impl From<DurableError> for ApiError {
    fn from(error: DurableError) -> Self {
        match error {
            DurableError::Index(error) => error.into(),
            DurableError::TooLarge => Self::invalid(error.to_string()),
            DurableError::Expired(_) => Self::new(tonic::Code::OutOfRange, error.to_string()),
            other => Self::new(tonic::Code::Unavailable, other.to_string()),
        }
    }
}

impl From<ApiError> for tonic::Status {
    fn from(error: ApiError) -> Self {
        tonic::Status::new(error.code, error.message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.code {
            tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
            tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
            tonic::Code::NotFound => StatusCode::NOT_FOUND,
            tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            tonic::Code::OutOfRange => StatusCode::GONE,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        };
        (status, Json(serde_json::json!({"error": self.message}))).into_response()
    }
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

fn position(p: Option<proto::Position>) -> Result<Position, ApiError> {
    p.map(|p| Position::new(p.lon, p.lat))
        .ok_or_else(|| ApiError::invalid("a position is required"))
}

fn ring(ring: proto::Ring) -> Vec<Position> {
    ring.positions
        .into_iter()
        .map(|p| Position::new(p.lon, p.lat))
        .collect()
}

fn polygon(polygon: Option<proto::Polygon>) -> Result<Vec<Vec<Position>>, ApiError> {
    Ok(polygon
        .ok_or_else(|| ApiError::invalid("a polygon is required"))?
        .rings
        .into_iter()
        .map(ring)
        .collect())
}

fn field_value(value: proto::FieldValue) -> Result<FieldValue, ApiError> {
    use proto::field_value::Value;
    Ok(match value.value {
        Some(Value::BoolValue(v)) => FieldValue::Bool(v),
        Some(Value::IntValue(v)) => FieldValue::Int(v),
        Some(Value::FloatValue(v)) => FieldValue::Float(v),
        Some(Value::TextValue(v)) => FieldValue::Text(v),
        None => return Err(ApiError::invalid("a field value is required")),
    })
}

fn to_field_value(value: FieldValue) -> proto::FieldValue {
    use proto::field_value::Value;
    proto::FieldValue {
        value: Some(match value {
            FieldValue::Bool(v) => Value::BoolValue(v),
            FieldValue::Int(v) => Value::IntValue(v),
            FieldValue::Float(v) => Value::FloatValue(v),
            FieldValue::Text(v) => Value::TextValue(v),
        }),
    }
}

fn entity(entity: proto::Entity) -> Result<Entity, ApiError> {
    use proto::geometry::Shape;
    let geometry = match entity.geometry.and_then(|g| g.shape) {
        Some(Shape::Point(p)) => Geometry::Point(Position::new(p.lon, p.lat)),
        Some(Shape::Line(line)) => Geometry::Line(ring(line)),
        Some(Shape::Polygon(p)) => Geometry::Polygon(polygon(Some(p))?),
        None => return Err(ApiError::invalid("an entity needs a geometry")),
    };
    Ok(Entity {
        collection: entity.collection,
        id: entity.id,
        geometry,
        ts_ms: entity.ts_ms,
        speed: entity.speed,
        heading: entity.heading,
        ttl_ms: entity.ttl_ms,
        fields: entity
            .fields
            .into_iter()
            .map(|(name, value)| Ok((name, field_value(value)?)))
            .collect::<Result<BTreeMap<_, _>, ApiError>>()?,
    })
}

fn to_entity(entity: Entity) -> proto::Entity {
    use proto::geometry::Shape;
    let to_ring = |positions: Vec<Position>| proto::Ring {
        positions: positions
            .into_iter()
            .map(|p| proto::Position {
                lon: p.lon,
                lat: p.lat,
            })
            .collect(),
    };
    let shape = match entity.geometry {
        Geometry::Point(p) => Shape::Point(proto::Position {
            lon: p.lon,
            lat: p.lat,
        }),
        Geometry::Line(line) => Shape::Line(to_ring(line)),
        Geometry::Polygon(rings) => Shape::Polygon(proto::Polygon {
            rings: rings.into_iter().map(to_ring).collect(),
        }),
    };
    proto::Entity {
        collection: entity.collection,
        id: entity.id,
        geometry: Some(proto::Geometry { shape: Some(shape) }),
        ts_ms: entity.ts_ms,
        speed: entity.speed,
        heading: entity.heading,
        ttl_ms: entity.ttl_ms,
        fields: entity
            .fields
            .into_iter()
            .map(|(name, value)| (name, to_field_value(value)))
            .collect(),
    }
}

pub(crate) fn query_request(request: proto::QueryRequest) -> Result<QueryRequest, ApiError> {
    use proto::query_request::Query as Kind;
    let query = match request.query {
        Some(Kind::Bbox(b)) => Query::Bbox([b.west, b.south, b.east, b.north]),
        Some(Kind::Radius(r)) => Query::Radius {
            center: position(r.center)?,
            meters: r.meters,
        },
        Some(Kind::Polygon(p)) => Query::Polygon {
            rings: polygon(p.polygon)?,
            relation: if p.contains {
                Relation::Contains
            } else {
                Relation::Intersects
            },
        },
        Some(Kind::Nearest(n)) => Query::Nearest {
            center: position(n.center)?,
            k: n.k as usize,
        },
        None => return Err(ApiError::invalid("a query is required")),
    };
    Ok(QueryRequest {
        collection: request.collection,
        query,
        filters: filters(request.filters)?,
        limit: request.limit as usize,
        cursor: (!request.cursor.is_empty()).then_some(request.cursor),
    })
}

fn filters(filters: Vec<proto::Filter>) -> Result<Vec<Filter>, ApiError> {
    filters
        .into_iter()
        .map(|filter| {
            use proto::filter::Test;
            Ok(match filter.test {
                Some(Test::Equals(value)) => Filter::Eq(filter.field, field_value(value)?),
                Some(Test::Range(range)) => Filter::Range {
                    field: filter.field,
                    min: range.min,
                    max: range.max,
                },
                None => return Err(ApiError::invalid("a filter needs a test")),
            })
        })
        .collect()
}

fn to_filter(filter: Filter) -> proto::Filter {
    use proto::filter::Test;
    match filter {
        Filter::Eq(field, value) => proto::Filter {
            field,
            test: Some(Test::Equals(to_field_value(value))),
        },
        Filter::Range { field, min, max } => proto::Filter {
            field,
            test: Some(Test::Range(proto::Range { min, max })),
        },
    }
}

fn to_position(p: Position) -> proto::Position {
    proto::Position {
        lon: p.lon,
        lat: p.lat,
    }
}

fn fence(fence: Option<proto::Fence>) -> Result<Fence, ApiError> {
    use proto::fence::Shape as Kind;
    let fence = fence.ok_or_else(|| ApiError::invalid("a fence is required"))?;
    let shape = match fence.shape {
        Some(Kind::Circle(circle)) => Shape::Circle {
            center: position(circle.center)?,
            meters: circle.meters,
        },
        Some(Kind::Polygon(p)) => Shape::Polygon(polygon(Some(p))?),
        None => return Err(ApiError::invalid("a fence needs a shape")),
    };
    Ok(Fence {
        id: fence.id,
        shape,
        collections: fence.collections,
        filters: filters(fence.filters)?,
        dwell_ms: fence.dwell_ms,
        inside_per_second: fence.inside_per_second,
    })
}

fn to_fence(fence: Fence) -> proto::Fence {
    use proto::fence::Shape as Kind;
    proto::Fence {
        id: fence.id,
        shape: Some(match fence.shape {
            Shape::Circle { center, meters } => Kind::Circle(proto::Radius {
                center: Some(to_position(center)),
                meters,
            }),
            Shape::Polygon(rings) => Kind::Polygon(proto::Polygon {
                rings: rings
                    .into_iter()
                    .map(|ring| proto::Ring {
                        positions: ring.into_iter().map(to_position).collect(),
                    })
                    .collect(),
            }),
        }),
        collections: fence.collections,
        filters: fence.filters.into_iter().map(to_filter).collect(),
        dwell_ms: fence.dwell_ms,
        inside_per_second: fence.inside_per_second,
    }
}

fn to_event(event: FenceEvent) -> proto::FenceEvent {
    let kind = match event.kind {
        EventKind::Enter => proto::EventKind::Enter,
        EventKind::Exit => proto::EventKind::Exit,
        EventKind::Inside => proto::EventKind::Inside,
        EventKind::Dwell => proto::EventKind::Dwell,
    };
    proto::FenceEvent {
        lsn: event.lsn,
        index: event.index,
        fence: event.fence,
        collection: event.collection,
        entity: event.entity,
        kind: kind as i32,
        ts_ms: event.ts_ms,
        position: Some(to_position(event.position)),
    }
}

/// A page size: the requested one, or the largest when none is given.
fn page(limit: u32) -> usize {
    match limit {
        0 => MAX_LIMIT,
        limit => (limit as usize).min(MAX_LIMIT),
    }
}

/// The shared core both transports call.
#[derive(Clone)]
pub struct Api {
    engine: Arc<DurableEngine>,
    auth: Arc<dyn Authenticator>,
}

impl Api {
    pub fn new(engine: Arc<DurableEngine>, auth: Arc<dyn Authenticator>) -> Self {
        Self { engine, auth }
    }

    pub(crate) fn namespace(&self, authorization: Option<&str>) -> Result<Namespace, ApiError> {
        Ok(Namespace::of(
            &self.auth.authenticate(bearer(authorization))?,
        ))
    }

    /// Run engine work off the async workers: writes wait on disk syncs.
    pub(crate) async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce(&DurableEngine) -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        let engine = self.engine.clone();
        tokio::task::spawn_blocking(move || work(&engine))
            .await
            .map_err(|_| ApiError::new(tonic::Code::Internal, "request task failed"))?
    }

    pub async fn upsert(
        &self,
        authorization: Option<&str>,
        request: proto::UpsertRequest,
    ) -> Result<proto::UpsertResponse, ApiError> {
        let ns = self.namespace(authorization)?;
        // Each entity gets its own outcome: one that does not convert is
        // refused alone, and the rest are written.
        let converted: Vec<Result<Entity, String>> = request
            .entities
            .into_iter()
            .map(|e| entity(e).map_err(|error| error.message))
            .collect();
        let valid: Vec<Entity> = converted
            .iter()
            .filter_map(|e| e.as_ref().ok().cloned())
            .collect();
        let mut outcomes = self
            .blocking(move |engine| Ok(engine.upsert_batch(&ns, valid, now_ms())?))
            .await?
            .into_iter();
        let result = |outcome: proto::Outcome, error: String| proto::UpsertResult {
            outcome: outcome as i32,
            error,
        };
        Ok(proto::UpsertResponse {
            results: converted
                .into_iter()
                .map(|converted| match converted {
                    Err(error) => result(proto::Outcome::Refused, error),
                    Ok(_) => match outcomes.next() {
                        Some(Ok(Upsert::Inserted)) => {
                            result(proto::Outcome::Inserted, String::new())
                        }
                        Some(Ok(Upsert::Updated)) => result(proto::Outcome::Updated, String::new()),
                        Some(Ok(Upsert::Stale)) => result(proto::Outcome::Stale, String::new()),
                        Some(Err(error)) => result(proto::Outcome::Refused, error.to_string()),
                        None => result(proto::Outcome::Refused, "no outcome".into()),
                    },
                })
                .collect(),
        })
    }

    pub async fn delete(
        &self,
        authorization: Option<&str>,
        request: proto::DeleteRequest,
    ) -> Result<proto::DeleteResponse, ApiError> {
        let ns = self.namespace(authorization)?;
        let removed = self
            .blocking(move |engine| {
                Ok(engine.delete(&ns, &request.collection, &request.id, now_ms())?)
            })
            .await?;
        Ok(proto::DeleteResponse { removed })
    }

    pub async fn get(
        &self,
        authorization: Option<&str>,
        request: proto::GetRequest,
    ) -> Result<proto::GetResponse, ApiError> {
        let ns = self.namespace(authorization)?;
        let found = self
            .blocking(move |engine| {
                Ok(engine.get(&ns, &request.collection, &request.id, now_ms())?)
            })
            .await?;
        Ok(proto::GetResponse {
            entity: found.map(to_entity),
        })
    }

    pub async fn query(
        &self,
        authorization: Option<&str>,
        request: proto::QueryRequest,
    ) -> Result<proto::QueryResponse, ApiError> {
        let ns = self.namespace(authorization)?;
        let request = query_request(request)?;
        let page = self
            .blocking(move |engine| Ok(engine.query(&ns, &request, now_ms())?))
            .await?;
        Ok(proto::QueryResponse {
            entities: page.entities.into_iter().map(to_entity).collect(),
            next_cursor: page.next_cursor.unwrap_or_default(),
        })
    }

    pub async fn status(
        &self,
        authorization: Option<&str>,
    ) -> Result<proto::StatusResponse, ApiError> {
        let ns = self.namespace(authorization)?;
        let namespace = ns.as_str().to_string();
        let count = self
            .blocking(move |engine| Ok(engine.entity_count(&ns)?))
            .await?;
        Ok(proto::StatusResponse {
            version: env!("CARGO_PKG_VERSION").into(),
            namespace,
            entity_count: count as u64,
        })
    }
}

impl Api {
    pub async fn put_fence(
        &self,
        authorization: Option<&str>,
        request: proto::PutFenceRequest,
    ) -> Result<proto::PutFenceResponse, ApiError> {
        let ns = self.namespace(authorization)?;
        let fence = fence(request.fence)?;
        let replaced = self
            .blocking(move |engine| Ok(engine.put_fence(&ns, fence, now_ms())?))
            .await?;
        Ok(proto::PutFenceResponse { replaced })
    }

    pub async fn get_fence(
        &self,
        authorization: Option<&str>,
        request: proto::GetFenceRequest,
    ) -> Result<proto::GetFenceResponse, ApiError> {
        let ns = self.namespace(authorization)?;
        let found = self
            .blocking(move |engine| Ok(engine.get_fence(&ns, &request.id)?))
            .await?;
        Ok(proto::GetFenceResponse {
            fence: found.map(to_fence),
        })
    }

    pub async fn delete_fence(
        &self,
        authorization: Option<&str>,
        request: proto::DeleteFenceRequest,
    ) -> Result<proto::DeleteFenceResponse, ApiError> {
        let ns = self.namespace(authorization)?;
        let removed = self
            .blocking(move |engine| Ok(engine.delete_fence(&ns, &request.id, now_ms())?))
            .await?;
        Ok(proto::DeleteFenceResponse { removed })
    }

    pub async fn list_fences(
        &self,
        authorization: Option<&str>,
        request: proto::ListFencesRequest,
    ) -> Result<proto::ListFencesResponse, ApiError> {
        let ns = self.namespace(authorization)?;
        let fences = self
            .blocking(move |engine| {
                let after = (!request.after_id.is_empty()).then_some(request.after_id.as_str());
                Ok(engine.list_fences(&ns, after, page(request.limit))?)
            })
            .await?;
        Ok(proto::ListFencesResponse {
            fences: fences.into_iter().map(to_fence).collect(),
        })
    }

    pub async fn list_events(
        &self,
        authorization: Option<&str>,
        request: proto::ListEventsRequest,
    ) -> Result<proto::ListEventsResponse, ApiError> {
        let ns = self.namespace(authorization)?;
        let after = request.after_lsn.map(|lsn| (lsn, request.after_index));
        let events = self
            .blocking(move |engine| {
                Ok(engine.events(&ns, after, &request.fences, page(request.limit))?)
            })
            .await?;
        Ok(proto::ListEventsResponse {
            events: events.events.into_iter().map(to_event).collect(),
            through_lsn: Some(events.through.0),
            through_index: events.through.1,
        })
    }

    /// Stream events from the resume point on, then live. The first read
    /// happens before the stream starts, so an expired resume point fails
    /// the call itself. A later failure (the reader fell behind the
    /// retention, or the namespace was sealed) ends the stream with it.
    pub async fn subscribe(
        &self,
        authorization: Option<&str>,
        request: proto::SubscribeRequest,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<proto::FenceEvent, ApiError>>, ApiError> {
        const BATCH: usize = 256;
        let ns = self.namespace(authorization)?;
        let engine = self.engine.clone();
        let fences = Arc::new(request.fences);
        let mut cursor = request.after_lsn.map(|lsn| (lsn, request.after_index));
        // Marked seen before each read, so a publish after it wakes the wait.
        let mut published = engine.watch_events();
        published.borrow_and_update();
        // Reads take the engine's locks, so they run off the async workers.
        let read = move |cursor: Option<(u64, u32)>| {
            let (engine, ns, fences) = (engine.clone(), ns.clone(), fences.clone());
            async move {
                tokio::task::spawn_blocking(move || engine.events(&ns, cursor, &fences, BATCH))
                    .await
                    .map_err(|_| ApiError::new(tonic::Code::Internal, "read task failed"))?
                    .map_err(ApiError::from)
            }
        };
        let mut page = read(cursor).await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(BATCH);
        tokio::spawn(async move {
            loop {
                // Nothing read past the cursor: wait for a publish. A page
                // of only filtered-out events still moves the cursor on.
                if Some(page.through) == cursor {
                    tokio::select! {
                        changed = published.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                        () = sender.closed() => return,
                    }
                }
                for event in page.events.drain(..) {
                    if sender.send(Ok(to_event(event))).await.is_err() {
                        return;
                    }
                }
                cursor = Some(page.through);
                published.borrow_and_update();
                page = match read(cursor).await {
                    Ok(batch) => batch,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
            }
        });
        Ok(receiver)
    }
}

pub(crate) fn metadata_auth<T>(request: &tonic::Request<T>) -> Option<String> {
    request
        .metadata()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

#[tonic::async_trait]
impl Ezu for Api {
    type SubscribeStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<proto::FenceEvent, tonic::Status>> + Send>,
    >;

    async fn subscribe(
        &self,
        request: tonic::Request<proto::SubscribeRequest>,
    ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        use tokio_stream::StreamExt;
        let auth = metadata_auth(&request);
        let events = Api::subscribe(self, auth.as_deref(), request.into_inner()).await?;
        let stream = tokio_stream::wrappers::ReceiverStream::new(events)
            .map(|event| event.map_err(tonic::Status::from));
        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn upsert(
        &self,
        request: tonic::Request<proto::UpsertRequest>,
    ) -> Result<tonic::Response<proto::UpsertResponse>, tonic::Status> {
        let auth = metadata_auth(&request);
        Ok(tonic::Response::new(
            Api::upsert(self, auth.as_deref(), request.into_inner()).await?,
        ))
    }

    async fn delete(
        &self,
        request: tonic::Request<proto::DeleteRequest>,
    ) -> Result<tonic::Response<proto::DeleteResponse>, tonic::Status> {
        let auth = metadata_auth(&request);
        Ok(tonic::Response::new(
            Api::delete(self, auth.as_deref(), request.into_inner()).await?,
        ))
    }

    async fn get(
        &self,
        request: tonic::Request<proto::GetRequest>,
    ) -> Result<tonic::Response<proto::GetResponse>, tonic::Status> {
        let auth = metadata_auth(&request);
        Ok(tonic::Response::new(
            Api::get(self, auth.as_deref(), request.into_inner()).await?,
        ))
    }

    async fn query(
        &self,
        request: tonic::Request<proto::QueryRequest>,
    ) -> Result<tonic::Response<proto::QueryResponse>, tonic::Status> {
        let auth = metadata_auth(&request);
        Ok(tonic::Response::new(
            Api::query(self, auth.as_deref(), request.into_inner()).await?,
        ))
    }

    async fn status(
        &self,
        request: tonic::Request<proto::StatusRequest>,
    ) -> Result<tonic::Response<proto::StatusResponse>, tonic::Status> {
        let auth = metadata_auth(&request);
        Ok(tonic::Response::new(
            Api::status(self, auth.as_deref()).await?,
        ))
    }
    async fn put_fence(
        &self,
        request: tonic::Request<proto::PutFenceRequest>,
    ) -> Result<tonic::Response<proto::PutFenceResponse>, tonic::Status> {
        let auth = metadata_auth(&request);
        Ok(tonic::Response::new(
            Api::put_fence(self, auth.as_deref(), request.into_inner()).await?,
        ))
    }

    async fn get_fence(
        &self,
        request: tonic::Request<proto::GetFenceRequest>,
    ) -> Result<tonic::Response<proto::GetFenceResponse>, tonic::Status> {
        let auth = metadata_auth(&request);
        Ok(tonic::Response::new(
            Api::get_fence(self, auth.as_deref(), request.into_inner()).await?,
        ))
    }

    async fn delete_fence(
        &self,
        request: tonic::Request<proto::DeleteFenceRequest>,
    ) -> Result<tonic::Response<proto::DeleteFenceResponse>, tonic::Status> {
        let auth = metadata_auth(&request);
        Ok(tonic::Response::new(
            Api::delete_fence(self, auth.as_deref(), request.into_inner()).await?,
        ))
    }

    async fn list_fences(
        &self,
        request: tonic::Request<proto::ListFencesRequest>,
    ) -> Result<tonic::Response<proto::ListFencesResponse>, tonic::Status> {
        let auth = metadata_auth(&request);
        Ok(tonic::Response::new(
            Api::list_fences(self, auth.as_deref(), request.into_inner()).await?,
        ))
    }

    async fn list_events(
        &self,
        request: tonic::Request<proto::ListEventsRequest>,
    ) -> Result<tonic::Response<proto::ListEventsResponse>, tonic::Status> {
        let auth = metadata_auth(&request);
        Ok(tonic::Response::new(
            Api::list_events(self, auth.as_deref(), request.into_inner()).await?,
        ))
    }
}

/// `GET /v1/events` parameters: `after_lsn`, `after_index`, `limit`, and
/// `fence` once per fence to read.
fn events_params(query: &str) -> Result<proto::ListEventsRequest, ApiError> {
    let pairs: Vec<(String, String)> =
        serde_urlencoded::from_str(query).map_err(|error| ApiError::invalid(error.to_string()))?;
    let mut request = proto::ListEventsRequest::default();
    let number = |value: &str| {
        value
            .parse::<u64>()
            .map_err(|_| ApiError::invalid(format!("not a number: {value}")))
    };
    for (name, value) in pairs {
        match name.as_str() {
            "after_lsn" => request.after_lsn = Some(number(&value)?),
            "after_index" => {
                request.after_index = u32::try_from(number(&value)?)
                    .map_err(|_| ApiError::invalid("after_index is too large"))?;
            }
            "limit" => request.limit = u32::try_from(number(&value)?).unwrap_or(u32::MAX),
            "fence" => request.fences.push(value),
            _ => return Err(ApiError::invalid(format!("unknown parameter {name}"))),
        }
    }
    Ok(request)
}

fn header_auth(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
}

/// The HTTP/JSON routes.
pub fn http_router(api: Api) -> axum::Router {
    axum::Router::new()
        .route(
            "/v1/entities",
            post(
                |State(api): State<Api>,
                 headers: HeaderMap,
                 Json(body): Json<proto::UpsertRequest>| async move {
                    api.upsert(header_auth(&headers), body).await.map(Json)
                },
            ),
        )
        .route(
            "/v1/entities/{collection}/{id}",
            get(
                |State(api): State<Api>,
                 headers: HeaderMap,
                 Path((collection, id)): Path<(String, String)>| async move {
                    api.get(header_auth(&headers), proto::GetRequest { collection, id })
                        .await
                        .map(Json)
                },
            )
            .delete(
                |State(api): State<Api>,
                 headers: HeaderMap,
                 Path((collection, id)): Path<(String, String)>| async move {
                    api.delete(
                        header_auth(&headers),
                        proto::DeleteRequest { collection, id },
                    )
                    .await
                    .map(Json)
                },
            ),
        )
        .route(
            "/v1/query",
            post(
                |State(api): State<Api>,
                 headers: HeaderMap,
                 Json(body): Json<proto::QueryRequest>| async move {
                    api.query(header_auth(&headers), body).await.map(Json)
                },
            ),
        )
        .route(
            "/v1/fences",
            get(
                |State(api): State<Api>,
                 headers: HeaderMap,
                 Params(params): Params<proto::ListFencesRequest>| async move {
                    api.list_fences(header_auth(&headers), params)
                        .await
                        .map(Json)
                },
            ),
        )
        .route(
            "/v1/fences/{id}",
            put(
                |State(api): State<Api>,
                 headers: HeaderMap,
                 Path(id): Path<String>,
                 Json(mut body): Json<proto::Fence>| async move {
                    // The path names the fence.
                    body.id = id;
                    api.put_fence(
                        header_auth(&headers),
                        proto::PutFenceRequest { fence: Some(body) },
                    )
                    .await
                    .map(Json)
                },
            )
            .get(
                |State(api): State<Api>, headers: HeaderMap, Path(id): Path<String>| async move {
                    api.get_fence(header_auth(&headers), proto::GetFenceRequest { id })
                        .await
                        .map(Json)
                },
            )
            .delete(
                |State(api): State<Api>, headers: HeaderMap, Path(id): Path<String>| async move {
                    api.delete_fence(header_auth(&headers), proto::DeleteFenceRequest { id })
                        .await
                        .map(Json)
                },
            ),
        )
        .route(
            "/v1/events",
            get(
                |State(api): State<Api>, headers: HeaderMap, RawQuery(query): RawQuery| async move {
                    let params = events_params(query.as_deref().unwrap_or(""))?;
                    api.list_events(header_auth(&headers), params)
                        .await
                        .map(Json)
                },
            ),
        )
        .route(
            "/v1/status",
            get(|State(api): State<Api>, headers: HeaderMap| async move {
                api.status(header_auth(&headers)).await.map(Json)
            }),
        )
        .with_state(api)
}

/// A TLS server configuration from PEM: the server's certificate chain and
/// key, and with `client_ca` a required client certificate signed by it
/// (mutual TLS). ALPN offers HTTP/2 and HTTP/1.1, so gRPC and plain HTTP
/// JSON clients both negotiate.
pub fn tls_config(
    cert_pem: &str,
    key_pem: &str,
    client_ca_pem: Option<&str>,
) -> Result<Arc<rustls::ServerConfig>, String> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let chain = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("server certificate: {error}"))?;
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
        .map_err(|error| format!("server key: {error}"))?;
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|error| error.to_string())?;
    let builder = match client_ca_pem {
        Some(ca) => {
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(ca.as_bytes()) {
                roots
                    .add(cert.map_err(|error| format!("client CA: {error}"))?)
                    .map_err(|error| format!("client CA: {error}"))?;
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()
            .map_err(|error| format!("client CA: {error}"))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let mut config = builder
        .with_single_cert(chain, key)
        .map_err(|error| format!("server identity: {error}"))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// Serve gRPC and HTTP/JSON on one listener until `shutdown` resolves.
/// With `tls`, both run over TLS (mutual TLS when it requires client
/// certificates). Each handshake runs in its own task with a time limit, so
/// a slow client cannot hold up accepting others.
pub async fn serve(
    api: Api,
    listener: tokio::net::TcpListener,
    tls: Option<Arc<rustls::ServerConfig>>,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    // Flight batches are larger than API messages.
    let flight = arrow_flight::flight_service_server::FlightServiceServer::new(api.clone())
        .max_decoding_message_size(64 << 20);
    let routes = tonic::service::Routes::from(http_router(api.clone()))
        .add_service(EzuServer::new(api))
        .add_service(flight);
    let server = tonic::transport::Server::builder()
        .accept_http1(true)
        .add_routes(routes);
    let Some(config) = tls else {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        return server
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await;
    };
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    let (sender, receiver) = tokio::sync::mpsc::channel(64);
    let accepting = tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                continue;
            };
            let (acceptor, sender) = (acceptor.clone(), sender.clone());
            tokio::spawn(async move {
                let handshake = acceptor.accept(tcp);
                if let Ok(Ok(stream)) =
                    tokio::time::timeout(std::time::Duration::from_secs(10), handshake).await
                {
                    let _ = sender.send(Ok::<_, std::io::Error>(stream)).await;
                }
            });
        }
    });
    let incoming = tokio_stream::wrappers::ReceiverStream::new(receiver);
    let served = server
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await;
    accepting.abort();
    served
}
