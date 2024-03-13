use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::parse::{ParseClient, ParseCreated, ParseError, ParseObject};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum EslType {
    Hanshow,
    Pricer,
    EasyVCO
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GenericEsl {
    pub r#type: EslType,
    pub serial: String,
    pub printed: bool,
    #[serde(rename = "objectId")]
    pub object_id: Option<String>,
    #[serde(rename = "itemId")]
    /// The item id.
    ///
    /// It only exists for Pricer Esls
    pub item_id: Option<String>,
    /// The ESL id.
    ///
    /// It can be either a long string randomly generated for Hanshow or
    /// a barcode string for pricer
    #[serde(rename = "eslId")]
    pub id: String,
    pub nom: String,
    #[serde(rename = "nomScientifique")]
    pub nom_scientifique: String,
    pub prix: String,
    #[serde(rename = "infosPrix")]
    pub infos_prix: String,
    pub engin: Option<String>,
    pub zone: Option<String>,
    #[serde(rename = "zoneCode")]
    pub zone_code: Option<String>,
    #[serde(rename = "sousZone")]
    pub sous_zone: Option<String>,
    #[serde(rename = "sousZoneCode")]
    pub sous_zone_code: Option<String>,
    pub plu: String,
    pub taille: String,
    #[serde(rename = "congelInfos", skip_serializing_if = "Option::is_none")]
    pub congel_infos: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origine: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allergenes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    // peche/eleve/peche eau douce ....
    #[serde(skip_serializing_if = "Option::is_none")]
    pub production: Option<String>,
    pub tva: Option<String>,
    pub categorie: Option<i32>,
    pub achats: Option<f32>,
}

impl ParseObject for GenericEsl {
     async fn save(&self) -> Result<ParseCreated, ParseError> {
        let client = ParseClient::from_env();
        client
            .save("parse/classes/GenericEsl".to_string(), &self)
            .await
    }

    /// Default search methods will aim to find non printed Esls for a specific serial
     async fn find(serial: String) -> Result<Vec<Self>, ParseError>
    where
        Self: Sized,
    {
        let mut query: HashMap<String, String> = HashMap::new();
        query.insert("serial".into(), serial);
        query.insert("printed".into(), "false".into());
        let client = ParseClient::from_env();
        client
            .fetch("parse/classes/GenericEsl".to_string(), query)
            .await
    }

    /// We dont have to edit Esls Content, so edit will only change the printed status from fale to True
     async fn update(&mut self) -> Result<Self, ParseError> {
        if self.object_id.is_none() {
            return Err(ParseError::ObectId);
        }
        let mut payload: HashMap<String, bool> = HashMap::new();
        payload.insert("printed".into(), true);
        let client = ParseClient::from_env();
        client
            .update(
                format!(
                    "parse/classes/GenericEsl/{}",
                    self.object_id.clone().unwrap()
                ),
                payload,
            )
            .await?;
        self.printed = true;
        Ok(self.clone())
    }
}
#[derive(Debug, Serialize, Deserialize)]
struct DateQuery{
    #[serde(rename = "$gt")]
    gt: String,
    #[serde(rename = "$lt")]
    lt: String
}
#[derive(Debug, Serialize, Deserialize)]
struct Query {
    createdAt: DateQuery,
    serial: String
}

impl GenericEsl {
    /// Specific search methods will aim to find printed and non printed Esls for a specific serial for a specific date
    pub async fn find_by_date(serial: String, start_date: String, end_date: String) -> Result<Vec<Self>, ParseError>
    where
        Self: Sized,
    {
        let date_query= DateQuery {
            gt: start_date,
            lt: end_date
        };
        let query = Query {
            serial,
            createdAt: date_query
        };
        let client = ParseClient::from_env();
        client
            .fetch("parse/classes/GenericEsl".to_string(), query)
            .await
    }

}