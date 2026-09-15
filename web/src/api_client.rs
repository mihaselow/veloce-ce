use gloo_net::http::Request;
use serde::{Deserialize, Serialize};
use web_sys::RequestCredentials;

use crate::login::get_session_storage_item;

fn with_auth(mut req: gloo_net::http::RequestBuilder) -> gloo_net::http::RequestBuilder {
    req = req.credentials(RequestCredentials::Include);
    if let Some(key) = get_session_storage_item("veloce_controller_api_key") {
        req = req.header("X-API-KEY", &key);
    }
    req
}

pub fn api_get(url: &str) -> gloo_net::http::RequestBuilder {
    with_auth(Request::get(url))
}

pub fn api_post(url: &str) -> gloo_net::http::RequestBuilder {
    with_auth(Request::post(url))
}

pub fn api_delete(url: &str) -> gloo_net::http::RequestBuilder {
    with_auth(Request::delete(url))
}

pub fn fileserver_get(url: &str) -> gloo_net::http::RequestBuilder {
    api_get(url)
}

pub async fn fetch_ws_ticket(scope: &str, job_id: Option<u64>) -> Result<String, u16> {
    #[derive(Serialize)]
    struct TicketReq {
        scope: String,
        job_id: Option<u64>,
    }
    #[derive(Deserialize)]
    struct TicketResp {
        ticket: String,
    }

    let payload = TicketReq {
        scope: scope.to_string(),
        job_id,
    };

    let resp = match with_auth(Request::post("/api/v1/ws/ticket")).json(&payload) {
        Ok(req) => match req.send().await {
            Ok(resp) => resp,
            Err(_) => return Err(0),
        },
        Err(_) => return Err(0),
    };

    let status = resp.status();
    if resp.ok() {
        let data = resp.json::<TicketResp>().await.map_err(|_| status)?;
        Ok(data.ticket)
    } else {
        Err(status)
    }
}
