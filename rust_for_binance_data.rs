use chainquant::utils::logger;
use polars::prelude::SerReader;
use polars::prelude::{CsvReader, ParquetWriter};
use reqwest;
use std::fs::{self, File};
use std::path::Path;
use tokio;
use tokio::io::AsyncWriteExt;
use zip::ZipArchive;

async fn download(
    start_year: i32,
    start_month: u32,
    end_year: i32,
    end_month: u32,
    symbol: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut current_year = start_year;
    let mut current_month = start_month;

    while current_year < end_year || (current_year == end_year && current_month <= end_month) {
        let padded_month = format!("{:02}", current_month);
        let url = format!(
            "https://data.binance.vision/data/spot/monthly/aggTrades/{}/{}-aggTrades-{}-{}.zip",
            symbol, symbol, current_year, padded_month
        );

        log::info!("downloading {}...", url);
        let response = reqwest::get(&url).await?;

        if response.status().is_success() {
            let content = response.bytes().await?;
            let mut file = tokio::fs::File::create(format!(
                "./data/{}-aggTrades-{}-{}.zip",
                symbol, current_year, padded_month
            ))
            .await?;
            file.write_all(&content).await?;
        } else {
            log::error!("download failed {}", url);
        }

        if current_month == 12 {
            current_year += 1;
            current_month = 1;
        } else {
            current_month += 1;
        }
    }
    Ok(())
}

fn unzip_and_cut_fileds(
    start_year: i32,
    start_month: u32,
    end_year: i32,
    end_month: u32,
    symbol: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut current_year = start_year;
    let mut current_month = start_month;

    while current_year < end_year || (current_year == end_year && current_month <= end_month) {
        let padded_month = format!("{:02}", current_month);
        let zip_file = format!(
            "./data/{}-aggTrades-{}-{}.zip",
            symbol, current_year, padded_month
        );
        let csv_file = format!("{}-aggTrades-{}-{}.csv", symbol, current_year, padded_month);
        let output_file = format!(
            "./data/{}-aggTrades-{}-{}a.csv",
            symbol, current_year, padded_month
        );

        log::info!("Extracting {}...", zip_file);
        let file = File::open(&zip_file)?;
        let mut archive = ZipArchive::new(file)?;

        if let Ok(archive) = archive.by_name(&csv_file) {
            let mut output_csv = csv::Writer::from_path(&output_file)?;

            let mut rdr = csv::Reader::from_reader(archive);
            for result in rdr.records() {
                let record = result?;
                output_csv.write_record(&[&record[1], &record[2], &record[5]])?;
            }

            log::info!("Extraction successful. Deleting {}...", zip_file);
            fs::remove_file(&zip_file)?;

            log::info!("Performing column extraction...");

            log::info!("Deleting {}...", csv_file);
        } else {
            log::error!("Extraction failed for {}.", zip_file);
        }

        if current_month == 12 {
            current_year += 1;
            current_month = 1;
        } else {
            current_month += 1;
        }
    }

    Ok(())
}

fn csv_to_parquet(
    start_year: i32,
    start_month: u32,
    end_year: i32,
    end_month: u32,
    symbol: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut current_year = start_year;
    let mut current_month = start_month;

    while current_year < end_year || (current_year == end_year && current_month <= end_month) {
        let padded_month = format!("{:02}", current_month);
        let csv_file = format!(
            "./data/{}-aggTrades-{}-{}a.csv",
            symbol, current_year, padded_month
        );

        let path = Path::new(&csv_file);

        if path.exists() {
            // Read the CSV file into a DataFrame
            let mut df = CsvReader::from_path(path)?.has_header(false).finish()?;

            // Rename columns
            df.set_column_names(&["price", "qty", "timestamp"])?;

            // Write the DataFrame to a Parquet file
            let parquet_file = csv_file.replace(".csv", ".parquet");
            let mut file = File::create(&parquet_file).unwrap();
            ParquetWriter::new(&mut file).finish(&mut df).unwrap();

            log::info!("Converted {} to {}", csv_file, parquet_file);

            // Remove the original CSV file
            fs::remove_file(&csv_file)?;
            log::info!("Deleted {}", csv_file);
        } else {
            log::error!("The file {} does not exist", csv_file);
        }
        if current_month == 12 {
            current_year += 1;
            current_month = 1;
        } else {
            current_month += 1;
        }
    }

    Ok(())
}

async fn load_data() -> Result<(), Box<dyn std::error::Error>> {
    log::info!("=========================");
    let start_year = 2020;
    let start_month = 1;
    let end_year = 2020;
    let end_month = 2;
    let symbol = "BTCUSDT";
    // Download data
    download(start_year, start_month, end_year, end_month, &symbol)
        .await
        .map_err(|e| {
            log::error!("Download failed: {:?}", e);
            e
        })?;
    log::info!("Download finished");
    log::info!("=========================");

    // Unzip and cut fields
    unzip_and_cut_fileds(start_year, start_month, end_year, end_month, &symbol).map_err(|e| {
        log::error!("Unzip and cut fields failed: {:?}", e);
        e
    })?;
    log::info!("Unzip and cut finished");
    log::info!("=========================");

    // Convert CSV to Parquet
    csv_to_parquet(start_year, start_month, end_year, end_month, &symbol).map_err(|e| {
        log::error!("CSV to Parquet conversion failed: {:?}", e);
        e
    })?;
    log::info!("CSV to Parquet conversion finished");
    log::info!("=========================");

    Ok(())
}

pub async fn binance_main() -> Result<(), Box<dyn std::error::Error>> {
    logger::setup("log", "binance.log", false).expect("config log sys failed");
    load_data().await?;
    Ok(())
}
