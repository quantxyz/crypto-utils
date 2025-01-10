use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct ApiResponse {
    code: String,
    msg: String,
    data: Vec<Data>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Data {
    page: String,
    limit: String,
    #[serde(rename = "totalPage")]
    total_page: String,
    #[serde(rename = "positionList")]
    position_list: Vec<Position>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Position {
    rank: String,
    symbol: String,
    #[serde(rename = "holderAddress")]
    holder_address: String,
    amount: String,
}

pub async fn get_top_1000(chain_short_name: &str) {
    let client = reqwest::Client::new();
    let api_key = "hello"; // Replace with your actual API key
    let mut sum: f64 = 0.0;

    for page in 1..=10 {
        let url = format!("https://www.oklink.com/api/v5/explorer/address/native-token-position-list?chainShortName={}&limit=100&page={}", chain_short_name, page);

        let mut headers = HeaderMap::new();
        headers.insert("Ok-Access-Key", HeaderValue::from_static(api_key));

        let response = client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .expect("Failed to send request");

        let api_response: ApiResponse = response.json().await.expect("Failed to parse JSON");

        for data in api_response.data {
            for position in data.position_list {
                let amount: f64 = position
                    .amount
                    .parse()
                    .expect("Failed to parse amount as f64");
                sum += amount;
            }
        }
    }

    log::info!("Total sum of {} amounts: {}", chain_short_name, sum);
}
// oklink
// hello
