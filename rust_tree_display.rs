use std::path::Path;
use std::{env, fs};

fn print_directory_structure(path: &Path, level: usize) {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) => {
            eprintln!("Error reading directory: {}: {}", path.display(), e);
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                eprintln!("Error reading entry: {:?}", e);
                continue;
            }
        };
        let path = entry.path();
        let spacing = " ".repeat(level * 4); // 4 spaces per level for indentation
        if path.is_dir() {
            println!(
                "{}+-- {}",
                spacing,
                path.file_name().unwrap().to_string_lossy()
            );
            print_directory_structure(&path, level + 1);
        } else {
            println!(
                "{}|-- {}",
                spacing,
                path.file_name().unwrap().to_string_lossy()
            );
        }
    }
}

pub fn tree_main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let path = if args.len() < 3 {
        // 默认为当前目录下的 "src" 目录
        Path::new("./src")
    } else {
        // 获取命令行参数中指定的目录
        let path_str = &args[2];
        let path = Path::new(path_str);
        if !path.is_dir() {
            eprintln!("Error: '{}' is not a directory", path_str);
            std::process::exit(1);
        }
        path
    };
    println!("Directory structure of '{}':", path.display());
    print_directory_structure(path, 0);
    Ok(())
}
