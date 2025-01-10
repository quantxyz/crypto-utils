use std::env;
mod tools;
use tools::{binance_data, equal_candle_volume, thirdchain, top1000, tree, battery};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    // 定义一个包含所有有效动作的向量
    let valid_actions = vec![
        "binance_data".to_string(),
        "equal_candle".to_string(),
        "thirdchain".to_string(),
        "top1000".to_string(),
        "tree".to_string(),
        "batt".to_string(),
        "lzx".to_string(),
    ];

    if args.len() > 1 {
        let action = args[1].to_string();
        match action.as_str() {
            // 使用as_str()避免不必要的to_string()转换
            "binance_data" => binance_data::binance_main().await.expect("run failed"),
            "equal_candle" => equal_candle_volume::equal_main().expect("run failed"),
            "thirdchain" => thirdchain::third_main().await.expect("run failed"),
            "top1000" => top1000::top1000_main().await.expect("run failed"),
            "tree" => tree::tree_main().expect("run failed"),
            "batt" => battery::batt_main().await,
            _ => {
                println!("Invalid action. Available actions are: {:?}", valid_actions);
            }
        }
    } else {
        println!("No arguments provided. Available actions are: {:?}", valid_actions);
    }
    Ok(())
}

