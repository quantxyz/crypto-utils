use std::process::Command;
use std::str;
use tokio::time::{interval, Duration};
use chainquant::utils::logger;

pub async fn batt_main() {
    logger::setup("log", "batt.log", false).expect("config log sys failed");
    let mut previous_battery_percentage = None;

    let mut interval = interval(Duration::from_secs(60));

    loop {
        interval.tick().await;

        if let Some(battery_percentage) = get_battery_percentage() {
            if previous_battery_percentage != Some(battery_percentage) {
                previous_battery_percentage = Some(battery_percentage);
                log::info!("电池电量: {}%", battery_percentage);
                // if battery_percentage%5 == 0 {
                //     announce_battery_percentage(battery_percentage);
                // }
                if battery_percentage <= 20 {
                    say_message("battery is low, please charge");
                }
                if battery_percentage >= 80 {
                    say_message("The power is sufficient, please unplug the charger");
                }
            }
        } else {
            log::warn!("无法解析电池电量");
        }
    }
}

fn get_battery_percentage() -> Option<u32> {
    // 执行 `pmset -g batt` 命令
    let output = Command::new("pmset")
        .arg("-g")
        .arg("batt")
        .output()
        .expect("Failed to execute command");

    // 将输出转换为字符串
    let output_str = str::from_utf8(&output.stdout).expect("Invalid UTF-8 in command output");

    // 解析电池电量
    parse_battery_percentage(output_str)
}

fn parse_battery_percentage(output: &str) -> Option<u32> {
    for line in output.lines() {
        if line.contains("InternalBattery") {
            // 使用 ';' 分割行，并找到包含 '%' 的部分
            if let Some(persub_str) = line.split(';')
                                              .find(|s| s.trim().ends_with('%')) {
                // 去掉百分号并解析为数字
                if let Some(percent_str) = persub_str.split(')')
                                              .find(|s| s.trim().ends_with('%')) {
                    if let Ok(percentage) = percent_str.trim().trim_end_matches('%').parse::<u32>() {
                        return Some(percentage);
                    }
                }
                
            }
        }
    }
    None
}

fn announce_battery_percentage(percentage: u32) {
    let message = format!("current battery charge is {}%", percentage);
    Command::new("say")
        .arg(&message)
        .output()
        .expect("Failed to execute say command");
}

pub fn say_message(msg: &str) {
    Command::new("say")
        .arg(msg)
        .output()
        .expect("Failed to execute say command");
}
