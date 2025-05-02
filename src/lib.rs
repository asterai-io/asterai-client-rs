use futures::stream::StreamExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use derive_builder::Builder;
use log::error;
use reqwest::Client;
use reqwest_eventsource::{Event, EventSource};
use thiserror::Error;

const CHANNEL_BUFFER: usize = 16;
const API_BASE_URL: &str = "https://api.asterai.io";
// This can actually be any UUID at the moment.
const SSE_DATA_PREFIX_LLM_TOKEN: &str = "llm-token: ";

#[derive(Debug, Builder, Clone, Eq, PartialEq, Hash)]
pub struct QueryAgentArgs {
    pub content: String,
    pub agent_id: String,
    pub query_key: String,
    #[builder(default)]
    pub conversation_id: Option<String>,
    #[builder(default)]
    pub api_base_url: Option<String>,
}

#[derive(Debug, Error)]
pub enum QueryAgentError {
    #[error("SSE error: {0:#?}")]
    EventSource(reqwest_eventsource::Error),
    #[error("SSE creation error: {0:#?}")]
    EventSourceCreation(reqwest_eventsource::CannotCloneRequestError),
}

pub async fn query_agent(args: &QueryAgentArgs) -> Result<Receiver<String>, QueryAgentError> {
    let (tx, rx) = channel(CHANNEL_BUFFER);
    let request_builder = Client::new()
        .post(get_endpoint(&args.agent_id, args.conversation_id.as_deref(), args.api_base_url.as_deref()))
        .header("authorization", args.query_key.clone())
        .body(args.content.to_owned());
    let mut event_source = EventSource::new(request_builder).map_err(QueryAgentError::EventSourceCreation)?;
    let (initial_result_tx, initial_result_rx) = tokio::sync::oneshot::channel::<Result<(), QueryAgentError>>();
    tokio::spawn(async move {
        let mut initial_result_tx_opt: Option<tokio::sync::oneshot::Sender<Result<(), QueryAgentError>>> = Some(initial_result_tx);
        while let Some(event) = event_source.next().await {
            let token = match event {
                Ok(Event::Message(m)) => {
                    if m.data.starts_with(SSE_DATA_PREFIX_LLM_TOKEN) {
                        m.data[SSE_DATA_PREFIX_LLM_TOKEN.len()..].to_owned()
                    } else {
                        continue;
                    }
                }
                Err(e) => {
                    if let Some(tx) = initial_result_tx_opt.take() {
                        tx.send(Err(QueryAgentError::EventSource(e))).unwrap();
                    }
                    event_source.close();
                    break;
                }
                _ => {
                    continue;
                }
            };
            if let Err(e) = tx.send(token).await {
                error!("{e:#?}");
                break;
            }
            if let Some(tx) = initial_result_tx_opt.take() {
                tx.send(Ok(())).unwrap();
            }
        }
    });
    initial_result_rx.await.unwrap()?;
    Ok(rx)
}

fn get_endpoint(app_id: &str, conversation_id: Option<&str>, api_base_url: Option<&str>) -> String {
    let query_string = conversation_id
        .map(|s| format!("?conversation_id={s}"))
        .unwrap_or_else(|| String::new());
    let api_base_url = api_base_url.unwrap_or(API_BASE_URL);
    format!("{api_base_url}/app/{app_id}/query/sse{query_string}")
}
