use std::path::Path;

use polars::prelude::*;

pub fn read_df_file(path: impl AsRef<Path>) -> anyhow::Result<LazyFrame> {
    let Some(extension) = path.as_ref().extension() else {
        anyhow::bail!(
            "No extension at the name of the file `{}`",
            path.as_ref().to_string_lossy()
        );
    };
    Ok(match extension.to_string_lossy().as_ref() {
        "csv" => LazyCsvReader::new(&path)
            .with_has_header(true)
            .with_infer_schema_length(None)
            .finish()?,
        "json" => {
            let file = std::fs::File::options().read(true).open(&path)?;
            JsonReader::new(file).finish()?.lazy()
        }
        "ndjson" | "jsonl" => LazyJsonLineReader::new(&path)
            .with_infer_schema_length(None)
            .finish()?,
        _ => anyhow::bail!(
            "Unknown extension `{}` at the name of the file `{}`",
            extension.to_string_lossy(),
            path.as_ref().to_string_lossy()
        ),
    })
}

pub fn write_df_output(mut df: DataFrame, path: impl AsRef<Path>) -> anyhow::Result<()> {
    let Some(extension) = path.as_ref().extension() else {
        anyhow::bail!(
            "No extension at the name of the file `{}`",
            path.as_ref().to_string_lossy()
        );
    };
    let output = std::fs::File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    match extension.to_string_lossy().as_ref() {
        "csv" => CsvWriter::new(output).finish(&mut df)?,
        "json" => JsonWriter::new(output).finish(&mut df)?,
        "ndjson" | "jsonl" => {
            anyhow::bail!(
                "No `JsonLineWriter` available to write `{}`",
                path.as_ref().to_string_lossy()
            );
        }
        _ => anyhow::bail!(
            "Unknown extension `{}` at the name of the file `{}`",
            extension.to_string_lossy(),
            path.as_ref().to_string_lossy()
        ),
    }
    Ok(())
}
