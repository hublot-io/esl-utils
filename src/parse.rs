use custom_error::custom_error;
use http::{HeaderMap, HeaderValue};
use log::{debug, info};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, io};

custom_error! {
    /// An error that can occur when sending logs to ParsePlatform.
    ///
    /// This error can be seamlessly converted to an `io::Error` and `reqwest::Error` via a `From`
    /// implementation.
    pub ParseError
        Url = "An error occured while parsing the URL",
        Reqwest{source: reqwest::Error} = "An issue occured within this request: {source}",
        SerdeJson{source: serde_json::Error} = "An issue occured while converting the payload to JSON: {source}",
        Io{source: io::Error}= "An I/O error occured: {source}",
        Platform{ code: reqwest::StatusCode, cause: String} =  "An error occured sending log to ParsePlatform. status: {code}, cause: {cause}",
        ObectId = "This ParseObject have no objectId, please create it first",
        Error{source: tokio_postgres::Error} = "Postgres Error: {source}"
}

pub trait ParseObject {
    async fn save(&self) -> Result<ParseCreated, ParseError>;
    async fn find(serial: String) -> Result<Vec<Self>, ParseError>
    where
        Self: Sized;
    async fn update(&mut self) -> Result<Self, ParseError>
    where
        Self: Sized;
}
#[derive(Clone)]
pub struct ParseClient {
    pub(self) application_id: String,
    pub(self) api_key: Option<String>,
    pub(self) server_url: String,
}
#[derive(Deserialize, Serialize)]
pub struct ParseCreated {
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
}
/// The response format of Parse query API
#[derive(Deserialize, Serialize)]
pub struct QueryResponse<T> {
    results: Vec<T>,
}
/// The response format of Parse API errors
#[derive(Deserialize, Serialize)]
pub struct ParseErrorResponse {
    code: i32,
    error: String,
}
/// A really basic ParsePlatform Rest API client
impl ParseClient {
    pub fn new(application_id: String, api_key: Option<String>, server_url: String) -> Self {
        Self {
            application_id,
            api_key,
            server_url,
        }
    }

    /// Returns a reqwest client with parse Authentication headers set
    fn get_client(&self) -> Result<Client, ParseError> {
        let mut headers = HeaderMap::new();
        let application_id = HeaderValue::from_str(&self.application_id)
            .expect("Cannot encode application ID into a request header");
        if let Some(api_key) = &self.api_key {
            let key = HeaderValue::from_str(&api_key)
                .expect("Cannot encode application key into a request header");
            headers.append("X-Parse-REST-API-Key", key);
        }
        headers.append("X-Parse-Application-Id", application_id);
        debug!("Forged request headers Headers {:?}", headers);
        Ok(Client::builder().default_headers(headers).build()?)
    }

    /// Returns a new ParseClient by reading properties from the environment.
    ///
    /// * PARSE_APPLICATION_ID
    /// * PARSE_API_KEY
    /// * PARSE_SERVER_URL
    pub fn from_env() -> Self {
        let parse_application_id =
            env::var("PARSE_APPLICATION_ID").expect("env.PARSE_APPLICATION_ID is undefined");
        let parse_api_key = env::var("PARSE_API_KEY").ok();
        let parse_server_url =
            env::var("PARSE_SERVER_URL").expect("env.PARSE_SERVER_URL is undefined");
        ParseClient::new(parse_application_id, parse_api_key, parse_server_url)
    }

    /// Merges a parse object path with the server root url
    fn get_url(&self, path: String) -> String {
        let formatted = format!("{}/{}", self.server_url, path);
        info!("Formated url {}", formatted);
        formatted
    }

    /// Saves a ParseObject by sending a POST request to the Parse API
    pub async fn save<T: serde::Serialize + std::fmt::Debug>(
        &self,
        path: String,
        data: T,
    ) -> Result<ParseCreated, ParseError> {
        let client = self.get_client()?;
        debug!(
            "Attempting to save ParseObject: {:?}",
            serde_json::to_string(&data)
        );
        let response = client.post(self.get_url(path)).json(&data).send().await?;
        match response.status() {
            StatusCode::CREATED => {
                let created: ParseCreated = response.json().await?;
                Ok(created)
            }
            error_code => {
                // Extract the error content
                let err_json: ParseErrorResponse = response.json().await?;
                Err(ParseError::Platform {
                    code: error_code,
                    cause: err_json.error,
                })
            }
        }
    }
    /// Find one or many ParseObject(s) by sending a GET request to the Parse API
    ///
    /// Query format: {"playerName":"Sean Plott","cheatMode":false, "score":{"$gte":1000,"$lte":3000}}}
    /// https://docs.parseplatform.org/rest/guide/#basic-queries
    pub async fn fetch<T: for<'de> serde::Deserialize<'de>, U: for<'de> serde::Serialize>(
        &self,
        path: String,
        query: U,
    ) -> Result<Vec<T>, ParseError> {
        let client = self.get_client()?;
        let payload = serde_json::to_string(&query)?;
        let mut url = Url::parse(&self.get_url(path)).map_err(|_e| ParseError::Url)?;
        url.query_pairs_mut().append_pair("where", &payload);
        let response = client.get(url).send().await?;
        match response.status() {
            StatusCode::OK => {
                let results: QueryResponse<T> = response.json().await?;
                Ok(results.results)
            }
            error_code => {
                let err_json: ParseErrorResponse = response.json().await?;
                Err(ParseError::Platform {
                    code: error_code,
                    cause: err_json.error,
                })
            }
        }
    }

    /// Updates a ParseObject by sending a PUT request to the Parse API
    pub async fn update<T: serde::Serialize>(
        &self,
        path: String,
        data: T,
    ) -> Result<(), ParseError> {
        let client = self.get_client()?;
        let response = client.put(self.get_url(path)).json(&data).send().await?;
        match response.status() {
            StatusCode::OK => Ok(()),
            error_code => {
                let err_json: ParseErrorResponse = response.json().await?;
                Err(ParseError::Platform {
                    code: error_code,
                    cause: err_json.error,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn get_env() -> Vec<&'static str> {
        let parse_application_id = "PARSE_APPLICATION_ID";
        let parse_server_url = "PARSE_SERVER_URL";
        let parse_api_key = "PARSE_API_KEY";
        vec![parse_application_id, parse_server_url, parse_api_key]
    }

    fn fill_env(vars: Vec<&'static str>) {
        vars.iter().for_each(|&v| {
            env::set_var(v, v);
            assert!(env::var(v).unwrap() == v);
        });
    }
    #[test]
    fn from_env() {
        let vars = get_env();
        fill_env(vars.clone());
        let parse_application_id = vars[0];
        let parse_server_url = vars[1];
        let parse_api_key = vars[2];
        let client = ParseClient::from_env();
        assert!(client.application_id == parse_application_id);
        assert!(client.api_key == parse_api_key);
        assert!(client.server_url == parse_server_url);
    }

    #[test]
    #[should_panic]
    fn from_env_invalid() {
        let vars = get_env();
        vars.iter().for_each(|&v| {
            env::remove_var(v);
        });
        let _ = ParseClient::from_env();
    }

    #[test]
    fn get_url() {
        let vars = get_env();
        fill_env(vars.clone());

        let client = ParseClient::from_env();
        let formated = client.get_url("status".to_string());
        assert!(formated == *"PARSE_SERVER_URL/status");
    }

    #[test]
    fn get_client() {
        let vars = get_env();
        fill_env(vars.clone());

        let parse = ParseClient::from_env();
        let client = parse.get_client();
        assert!(client.is_ok());
    }
}
