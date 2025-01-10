use bigdecimal::{BigDecimal, FromPrimitive};
use regex::Regex;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::str::FromStr;

use chainquant::utils::logger;
// hello
// curl -H "X-Dune-API-Key:<api_key>" "https://api.dune.com/api/v1/query/1008481/results?limit=1000"

// https://etherscan.io/
// 5 call/second limit
// Up to 100,000 API calls/day
// All existing community endpoints
// Community support

#[derive(Deserialize, Debug)]
struct AccountData {
    // address: String,
    // nametag: String,
    balance: String,
    // percentage: String,
    // txncount: String,
}

#[derive(Deserialize, Debug)]
struct Account {
    account: String,
    balance: String,
}

#[derive(Deserialize, Debug)]
struct ApiResponse {
    status: String,
    message: String,
    result: Vec<Account>,
}

async fn get_balance(addresses: VecDeque<String>) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let api_key = "hello";
    let address_vec = addresses.into_iter().collect::<Vec<_>>(); // 将 VecDeque 转换为 Vec
    let chunks = address_vec.chunks(20); // 对 Vec 使用 chunks 方法 // 将地址分组，每组 20 个
    let mut sum = BigDecimal::from_str("0").unwrap(); // 初始化总和为 0
    let base: u64 = 10;
    let exponent: u32 = 18;
    let result = base.pow(exponent);
    let ether = BigDecimal::from_u64(result).unwrap();

    for chunk in chunks {
        let address_list = chunk.join(","); // 将地址列表转换成逗号分隔的字符串
        let url = format!(
            "https://api.etherscan.io/api?module=account&action=balancemulti&address={}&tag=latest&apikey={}",
            address_list, api_key
        );

        let response = client.get(&url).send().await?.json::<ApiResponse>().await?;
        if response.status == "1" {
            for account in response.result.iter() {
                let balance = BigDecimal::from_str(&account.balance).unwrap();
                // 将 wei 转换为 ether
                let balance_in_ether = balance / ether.clone();
                sum += balance_in_ether.clone(); // 累加余额
                log::info!(
                    "Address: {}, Balance: {}, Total: {}",
                    account.account,
                    balance_in_ether,
                    sum
                );
            }
        } else {
            log::error!("Error retrieving data: {}", response.message);
        }
    }
    log::info!("Total Top 1000 eth based dune sum: {}", sum);
    Ok(())
}

async fn eth_top_1000_from_etherscan() -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let mut sum: f64 = 0.0; // 初始化总和为 0
    for page in 1..=10 {
        let url = format!("https://etherscan.io/accounts/{}?ps=100", page);
        println!("Fetching URL: {}", url);

        let res = client.get(&url).send().await?.text().await?;
        let re = Regex::new(r"quickExportAccountsData = '(\[.*?\])'").unwrap();
        if let Some(caps) = re.captures(&res) {
            let json_str = &caps[1];
            let lowercased_str = json_str.to_lowercase();
            // 尝试解析 JSON 数据为 AccountData 结构体的 Vec
            let accounts: Vec<AccountData> = serde_json::from_str(&lowercased_str)?;
            for account in accounts.iter() {
                let eth_string = account.balance.clone();
                let parts: Vec<&str> = eth_string.split(' ').collect();
                let number_str = parts[0].replace(",", "");

                match number_str.parse::<f64>() {
                    Ok(number) => {
                        sum += number;
                    }
                    Err(e) => log::error!("Failed to parse number: {}", e),
                }
                // log::info!(
                //     "Address: {}, NameTag: {}, Balance: {}, Transactions: {}, Total: {}",
                //     account.address,
                //     account.nametag,
                //     account.balance,
                //     account.txncount,
                //     sum
                // );
            }
        } else {
            log::error!("quickExportAccountsData not found on page {}", page);
        }
    }
    log::info!("Total Top 1000 eth sum: {}", sum);
    Ok(())
}

async fn btc_top_1000(cookie: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();

    // Setting up headers
    let mut headers = HeaderMap::new();
    headers.insert("authority", HeaderValue::from_static("btc.cryptoid.info"));
    headers.insert("accept", HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"));
    headers.insert(
        "accept-language",
        HeaderValue::from_static("en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7"),
    );

    headers.insert("cache-control", HeaderValue::from_static("max-age=0"));
    let cookie_header_value = HeaderValue::from_str(cookie)?;
    headers.insert("cookie", cookie_header_value);
    headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    // URL and request with get method
    let url = "https://btc.cryptoid.info/btc/api.dws?q=rich";
    let res = client
        .get(url)
        .headers(headers)
        .send()
        .await?
        .json::<HashMap<String, Value>>()
        .await?;

    // Processing the JSON response
    if let Some(rich1000) = res.get("rich1000").and_then(|v| v.as_array()) {
        let sum: f64 = rich1000
            .iter()
            .filter_map(|item| item.get("amount").and_then(|a| a.as_f64()))
            .sum();
        log::info!("Total Top 1000 btc sum: {}", sum);
    }

    Ok(())
}

async fn eth_top_1000_address_from_dune() -> Result<VecDeque<String>, Box<dyn std::error::Error>> {
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
        .get("https://api.dune.com/api/v1/query/1008481/results?limit=1000")
        .headers(headers)
        .send()
        .await?;

    // 检查响应状态码
    if response.status().is_success() {
        // 解析响应为JSON
        let json_response = response.json::<serde_json::Value>().await?;
        let rows = json_response["result"]["rows"].as_array().unwrap();

        // 创建一个用于存储地址的Vec
        let mut addresses: VecDeque<String> = VecDeque::new();

        // 遍历rows中的每个对象
        for row in rows {
            if let Some(address) = row["address"].as_str() {
                addresses.push_back(address.to_string());
            }
        }
        Ok(addresses)
    } else {
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to fetch data",
        )) as Box<dyn std::error::Error>)
    }
}

async fn eth_from_dune() -> Result<(), Box<dyn std::error::Error>> {
    // fetch data
    match eth_top_1000_address_from_dune().await {
        Ok(addresses) => {
            if let Err(e) = get_balance(addresses).await {
                log::error!("Error processing balances: {}", e);
            }
        }
        Err(e) => {
            log::error!("Error fetching addresses data: {}", e);
        }
    }
    log::info!("fetch finished");
    Ok(())
}

pub async fn third_main() -> Result<(), Box<dyn std::error::Error>> {
    logger::setup("log", "thirdchain.log", false).expect("config log sys failed");
    let _ = eth_from_dune().await;
    let _ = eth_top_1000_from_etherscan().await;
    // cookie for btc.cryptoid.info to get BTC blockchain data
    let cookie = "cf_chl_rc_m=1; cf_chl_3=a256291b18c1501; cf_clearance=u0SRZkEPCC48gVk8RuGtOakMwyzrb5x3DZy9WRFoU40-1714985834-1.0.1.1-v4gO1PjJlUuI0uU3wGDS3M17.zCw_vTA2Z9_AAB3vV6zk1SXIP60B.GaTs6PqNmXceq0tzn6Y585JxqDvQhfag";
    btc_top_1000(cookie).await
}
