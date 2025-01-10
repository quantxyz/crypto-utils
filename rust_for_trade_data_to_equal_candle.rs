use arrow::array::{Float64Array, Int64Array};
use csv::WriterBuilder;
use glob::glob;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

use chainquant::utils::logger;

fn agg_trades_to_equal_volume_candle() -> Result<(), Box<dyn std::error::Error>> {
    // 根据你的文件存储模式调整 glob
    let pattern = "./data/BTCUSDT-aggTrades-*.parquet";
    let output_csv = "candle_equal_volume_data_1000.csv";
    let volume_threshold = 1000.0; // 设定每个 K 线的成交量阈值

    let mut wtr = WriterBuilder::new().from_path(output_csv)?;

    // 写入 CSV 头部
    wtr.write_record(&["timestamp", "open", "high", "low", "close", "volume"])?;

    let mut open = 0.0;
    let mut high = f64::MIN;
    let mut low = f64::MAX;
    let mut close = 0.0;
    let mut volume = 0.0;
    let mut unix_time = 0;

    for entry in glob(pattern)? {
        let path = entry?;
        log::info!("{}", path.display());
        // 读取 parquet 文件
        let file = File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut arrow_reader = builder.build().unwrap();

        // 处理记录批次
        while let Some(batch) = arrow_reader.next() {
            let batch = batch?;
            let price_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let qty_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let unix_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                let price = price_col.value(i);
                let qty = qty_col.value(i);
                let unix = unix_col.value(i);

                // 更新 K 线数据
                if volume == 0.0 {
                    open = price;
                    high = price;
                    low = price;
                    unix_time = unix;
                }

                high = high.max(price);
                low = low.min(price);
                volume += qty;

                // 当累积成交量达到阈值时
                if volume >= volume_threshold {
                    close = price;

                    // 写入 K 线数据到 CSV
                    wtr.write_record(&[
                        unix_time.to_string(),
                        open.to_string(),
                        high.to_string(),
                        low.to_string(),
                        close.to_string(),
                        volume.to_string(),
                    ])?;

                    // 重置 K 线数据
                    volume = 0.0;
                }
            }
        }
    } // for

    // 如果最后一个 K 线未完成，也将其写入 CSV 文件
    if volume > 0.0 {
        wtr.write_record(&[
            unix_time.to_string(),
            open.to_string(),
            high.to_string(),
            low.to_string(),
            close.to_string(),
            volume.to_string(),
        ])?;
    }

    wtr.flush()?;
    Ok(())
}

pub fn equal_main() -> Result<(), Box<dyn std::error::Error>> {
    logger::setup("log", "candle_equal_volume.log", false).expect("config log sys failed");
    let _ = agg_trades_to_equal_volume_candle();
    Ok(())
}
