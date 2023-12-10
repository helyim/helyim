use actix_web::{post, web, web::Data, Responder};
use web::Json;

use crate::raft::{store::Request, RaftServer};

/// Application API
///
/// This is where you place your application, you can use the example below to create your
/// API. The current implementation:
///
///  - `POST - /write` saves a value in a key and sync the nodes.
///  - `POST - /read` attempt to find a value from a given key.
#[post("/write")]
pub async fn write(app: Data<RaftServer>, req: Json<Request>) -> actix_web::Result<impl Responder> {
    let response = app.raft.client_write(req.0).await;
    Ok(Json(response))
}

#[post("/read")]
pub async fn read(app: Data<RaftServer>) -> actix_web::Result<impl Responder> {
    match app.raft.is_leader().await {
        Ok(_) => {
            let state_machine = app.store.state_machine.read().await;
            let value = state_machine.topology().read().await.max_volume_id();
            Ok(Json(Ok(value)))
        }
        Err(e) => Ok(Json(Err(e))),
    }
}
