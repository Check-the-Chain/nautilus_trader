use reqwest::Client;
use tracing::trace;

use crate::config::Config;
use crate::error::{Result, SdkError};

#[derive(Debug, Clone)]
pub struct LighterRestClient {
    client: Client,
    base_url: String,
    auth_token: Option<String>,
}

impl LighterRestClient {
    fn encode_form(form: &[(&str, &str)]) -> Result<String> {
        serde_urlencoded::to_string(form)
            .map_err(|e| SdkError::Other(format!("Failed to encode form body: {e}")))
    }

    pub fn new(config: &Config) -> Result<Self> {
        let mut builder = Client::builder().pool_max_idle_per_host(config.pool_size);

        if let Some(timeout_secs) = config.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs(timeout_secs));
        }

        if let Some(ref proxy_url) = config.proxy {
            let proxy =
                reqwest::Proxy::all(proxy_url).map_err(|e| SdkError::Other(e.to_string()))?;
            builder = builder.proxy(proxy);
        }

        let client = builder.build()?;

        Ok(Self {
            client,
            base_url: config.api_base_url(),
            auth_token: None,
        })
    }

    pub fn set_auth_token(&mut self, token: String) {
        self.auth_token = Some(token);
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.get(&url);
        if let Some(ref token) = self.auth_token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        let resp = req.send().await?;
        Self::handle_response(resp).await
    }

    pub async fn get_with_query<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.get(&url).query(query);
        if let Some(ref token) = self.auth_token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        let resp = req.send().await?;
        Self::handle_response(resp).await
    }

    pub async fn get_with_auth<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
        auth: &str,
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .get(&url)
            .query(query)
            .header("Authorization", auth)
            .send()
            .await?;
        Self::handle_response(resp).await
    }

    pub async fn post_form<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let encoded = Self::encode_form(form)?;
        let mut req = self.client.post(&url).header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        );
        if let Some(ref token) = self.auth_token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        let resp = req.body(encoded).send().await?;
        Self::handle_response(resp).await
    }

    pub async fn post_form_with_auth<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        form: &[(&str, &str)],
        auth: &str,
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let encoded = Self::encode_form(form)?;
        let resp = self
            .client
            .post(&url)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(encoded)
            .header("Authorization", auth)
            .send()
            .await?;
        Self::handle_response(resp).await
    }

    pub async fn post_json<T: serde::de::DeserializeOwned, B: serde::Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.post(&url).json(body);
        if let Some(ref token) = self.auth_token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        let resp = req.send().await?;
        Self::handle_response(resp).await
    }

    async fn handle_response<T: serde::de::DeserializeOwned>(resp: reqwest::Response) -> Result<T> {
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            // Try to parse as API error envelope
            if let Ok(err) = serde_json::from_str::<serde_json::Value>(&body)
                && let (Some(code), Some(message)) = (
                    err.get("code").and_then(|c| c.as_i64()),
                    err.get("message").and_then(|m| m.as_str()),
                )
            {
                return Err(SdkError::Api {
                    code,
                    message: message.to_string(),
                });
            }
            return Err(SdkError::Api {
                code: status.as_u16() as i64,
                message: body,
            });
        }
        let body = resp.text().await?;
        trace!(raw_body = %body, "REST response");
        serde_json::from_str(&body).map_err(Into::into)
    }
}
