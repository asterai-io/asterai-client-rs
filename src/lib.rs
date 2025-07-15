use derive_builder::Builder;
use futures::stream::StreamExt;
use log::error;
use reqwest::Client;
use reqwest_eventsource::{Event, EventSource};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver};

const CHANNEL_BUFFER: usize = 16;
const API_BASE_URL: &str = "https://api.asterai.io";
// This can actually be any UUID at the moment.
const SSE_DATA_PREFIX_LLM_TOKEN: &str = "llm-token: ";
const SSE_DATA_PREFIX_PLUGIN_OUTPUT: &str = "plugin-output: ";
const TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Debug, Builder, Clone, Eq, PartialEq, Hash)]
pub struct QueryAgentArgs {
    pub content: String,
    pub agent_id: String,
    pub query_key: String,
    #[builder(default)]
    pub conversation_id: Option<String>,
    #[builder(default)]
    pub api_base_url: Option<String>,
    #[builder(default)]
    pub timeout: Option<Duration>,
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
    let mut event_rx = query_agent_with_events(args).await?;
    while let Some(event) = event_rx.recv().await {
        match event {
            AppEvent::LlmToken(token) => {
                if let Err(e) = tx.send(token).await {
                    error!("{e:#?}");
                    break;
                }
            }
            _ => {}
        }
    }
    Ok(rx)
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub enum AppEvent {
    LlmToken(String),
    PluginOutput(PluginOutput),
}

#[derive(Debug, Eq, PartialEq, Clone, Deserialize, Serialize)]
pub struct PluginOutput {
    pub plugin: String,
    pub function: String,
    pub value: serde_json::Value,
}

pub async fn query_agent_with_events(
    args: &QueryAgentArgs,
) -> Result<Receiver<AppEvent>, QueryAgentError> {
    let (tx, rx) = channel(CHANNEL_BUFFER);
    let request_builder = Client::new()
        .post(get_endpoint(
            &args.agent_id,
            args.conversation_id.as_deref(),
            args.api_base_url.as_deref(),
        ))
        .header("authorization", args.query_key.clone())
        .timeout(args.timeout.unwrap_or(TIMEOUT))
        .body(args.content.to_owned());
    let mut event_source =
        EventSource::new(request_builder).map_err(QueryAgentError::EventSourceCreation)?;
    let (initial_result_tx, initial_result_rx) =
        tokio::sync::oneshot::channel::<Result<(), QueryAgentError>>();
    tokio::spawn(async move {
        let mut initial_result_tx_opt: Option<
            tokio::sync::oneshot::Sender<Result<(), QueryAgentError>>,
        > = Some(initial_result_tx);
        while let Some(event) = event_source.next().await {
            let app_event = match event {
                Ok(Event::Message(m)) => {
                    if m.data.starts_with(SSE_DATA_PREFIX_LLM_TOKEN) {
                        let llm_token = m.data[SSE_DATA_PREFIX_LLM_TOKEN.len()..].to_owned();
                        AppEvent::LlmToken(llm_token)
                    } else if m.data.starts_with(SSE_DATA_PREFIX_PLUGIN_OUTPUT) {
                        let serialized_plugin_output =
                            m.data[SSE_DATA_PREFIX_PLUGIN_OUTPUT.len()..].to_owned();
                        let Ok(plugin_output) = serde_json::from_str(&serialized_plugin_output)
                        else {
                            error!(
                                "failed to deserialize plugin output: {serialized_plugin_output}"
                            );
                            continue;
                        };
                        AppEvent::PluginOutput(plugin_output)
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
            if let Err(e) = tx.send(app_event).await {
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
