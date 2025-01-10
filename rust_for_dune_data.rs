use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};


// hello
// curl -H "X-Dune-API-Key:<api_key>" "https://api.dune.com/api/v1/query/1008481/results?limit=1000"

pub async fn sol_top_1000_from_dune() -> Result<(), Box<dyn std::error::Error>> {
    // 从环境变量获取API密钥
    let api_key = "hello";
    // 创建HTTP客户端
    let client = reqwest::Client::new();

    // 设置请求头部
    let mut headers = HeaderMap::new();
    headers.insert("X-Dune-API-Key", HeaderValue::from_str(&api_key).unwrap());
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    // 发起GET请求
    let response = client
        .get("https://api.dune.com/api/v1/query/3668764/results?limit=1000")
        .headers(headers)
        .send()
        .await?;

    // 检查响应状态码
    if response.status().is_success() {
        // 解析响应为JSON
        let json_response = response.json::<serde_json::Value>().await?;
        let rows = json_response["result"]["rows"].as_array().unwrap();

        let sum_balances: f64 = rows
            .iter()
            .map(|row| row["balance"].as_f64().unwrap())
            .sum();
        log::info!("Total sum of sol amounts: {}", sum_balances);
        Ok(())
    } else {
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to fetch data",
        )) as Box<dyn std::error::Error>)
    }
}
