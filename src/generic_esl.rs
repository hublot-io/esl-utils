use crate::parse::ParseError;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use tokio_postgres::{NoTls, Row};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug, ToSql, FromSql)]
pub enum EslType {
    Hanshow,
    Pricer,
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
    pub taille: Option<String>,
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
}

impl From<&Row> for GenericEsl {
    fn from(row: &Row) -> Self {
        Self {
            r#type: row.get("type"),
            serial: row.get("serial"),
            printed: row.get("printed"),
            object_id: row.get("objectId"),
            item_id: row.get("itemId"),
            id: row.get("eslId"),
            nom: row.get("nom"),
            nom_scientifique: row.get("nomScientifique"),
            prix: row.get("prix"),
            infos_prix: row.get("infosPrix"),
            engin: row.get("engin"),
            zone: row.get("zone"),
            zone_code: row.get("zoneCode"),
            sous_zone: row.get("sousZone"),
            sous_zone_code: row.get("sousZoneCode"),
            plu: row.get("plu"),
            taille: row.get("taille"),
            congel_infos: row.get("congelInfos"),
            origine: row.get("origine"),
            allergenes: row.get("allergenes"),
            label: row.get("label"),
            production: row.get("production"),
        }
    }
}

impl GenericEsl {
    pub async fn do_save(
        mut esl: GenericEsl,
        pool: Pool<PostgresConnectionManager<NoTls>>,
    ) -> Result<Self, ParseError> {
        let conn = pool
            .get()
            .await
            .expect("upload: cannot access to the conneciton pool");
        println!("esl {:?}", esl);
        let uuid = Uuid::new_v4().to_string();
        conn.execute("INSERT INTO esl
            (objectId, nom, nomScientifique, plu, congelInfos, type, origine, serial, printed, eslId, prix, zone, sousZone, engin, zoneCode, sousZoneCode, infosPrix, taille, production, allergenes, itemId, label, createdAt) VALUES
            ($1      ,$2  ,$3              ,$4    ,$5        ,$6     ,$7     ,$8      ,$9    ,$10  ,$11 , $12 , $13     , $14  ,$15      ,$16          , $17        ,$18  , $19       , $20       , $21   , $22  , now())",
        &[&uuid, &esl.nom, &esl.nom_scientifique, &esl.plu, &esl.congel_infos, &esl.r#type, &esl.origine, &esl.serial,&esl.printed,&esl.id,&esl.prix,&esl.zone,&esl.sous_zone, &esl.engin,&esl.zone_code,&esl.sous_zone_code, &esl.infos_prix,&esl.taille, &esl.production, &esl.allergenes,&esl.item_id, &esl.label]
        ).await?;
        esl.object_id = Some(uuid);
        Ok(esl)
    }

    pub async fn set_printed(
        mut esl: GenericEsl,
        pool: Pool<PostgresConnectionManager<NoTls>>,
    ) -> Result<Self, ParseError> {
        let conn = pool
            .get()
            .await
            .expect("upload: cannot access to the conneciton pool");

        conn.query(
            "UPDATE esl SET printed=true where objectId=$1",
            &[&esl.object_id],
        )
        .await?;
        esl.printed = true;
        Ok(esl)
    }

    pub async fn do_find(
        serial: String,
        pool: Pool<PostgresConnectionManager<NoTls>>,
    ) -> Result<Vec<Self>, ParseError> {
        let conn = pool
            .get()
            .await
            .expect("upload: cannot access to the conneciton pool");

        let rows = conn
            .query(
                "SELECT * FROM esl WHERE serial=$1::text AND printed = false",
                &[&serial],
            )
            .await?;
        let esls: Vec<GenericEsl> = rows.iter().map(GenericEsl::from).collect();
        Ok(esls)
    }

    /// Specific search methods will aim to find printed and non printed Esls for a specific serial for a specific date
    pub async fn find_by_date(
        serial: String,
        start_date: String,
        end_date: String,
        pool: Pool<PostgresConnectionManager<NoTls>>,
    ) -> Result<Vec<Self>, ParseError> {
        let conn = pool
            .get()
            .await
            .expect("upload: cannot access to the conneciton pool");
        let rows = conn.query("SELECT * FROM esl WHERE serial=$1 AND createdAt > TO_TIMESTAMP($2,'YYYY-MM-DD HH24:MI:SS:MS') AND createdAt < TO_TIMESTAMP($3,'YYYY-MM-DD HH24:MI:SS:MS')",&[&serial,&start_date, &end_date]).await?;
        let esls: Vec<GenericEsl> = rows.iter().map(GenericEsl::from).collect();
        Ok(esls)
    }
}
