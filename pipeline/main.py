from pipeline.logger import write_log
from pipeline.file_manager import (
    get_latest_incoming_file,
    prepare_file_paths,
    copy_to_raw,
    archive_incoming_file
)
from pipeline.spark_utils import create_spark_session
from pipeline.transform import (
    standardize_column_names,
    convert_data_types,
    add_features
)
from pipeline.validate import (
    apply_business_rules,
    split_valid_rejected
)


def main():
    write_log("Pipeline started")

    incoming_file_path = get_latest_incoming_file()
    paths = prepare_file_paths(incoming_file_path)

    copy_to_raw(
        incoming_file_path,
        paths["raw_file_path"]
    )

    spark = create_spark_session()
    write_log("Spark session started")

    df = spark.read.option("header", True) \
        .option("inferSchema", True) \
        .option("multiLine", True) \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("encoding", "ISO-8859-1") \
        .csv(paths["raw_file_path"])

    write_log(f"Raw file loaded into Spark: {paths['raw_file_path']}")

    df = standardize_column_names(df)
    write_log("Column names standardized")

    df = convert_data_types(df)
    write_log("Data type conversions completed")

    df = add_features(df)
    write_log("Feature engineering completed")

    df = apply_business_rules(df)
    write_log("Business rule validation completed")

    valid_df, rejected_df = split_valid_rejected(df)

    total_records = df.count()
    valid_records = valid_df.count()
    rejected_records = rejected_df.count()

    write_log(f"Input file: {incoming_file_path}")
    write_log(f"Raw file: {paths['raw_file_path']}")
    write_log(f"Output folder name: {paths['folder_name']}")
    write_log(f"Total records: {total_records}")
    write_log(f"Valid records: {valid_records}")
    write_log(f"Rejected records: {rejected_records}")

    print("Sample rejected records:")
    rejected_df.select(
        "row_id",
        "order_id",
        "quantity",
        "discount",
        "sales",
        "order_date",
        "ship_date",
        "rejection_reason"
    ).show(10, truncate=False)

    valid_df.write.mode("overwrite").parquet(paths["processed_output_path"])
    rejected_df.write.mode("overwrite").parquet(paths["rejected_output_path"])

    write_log(f"Valid processed data saved to: {paths['processed_output_path']}")
    write_log(f"Rejected data saved to: {paths['rejected_output_path']}")

    archive_incoming_file(
        incoming_file_path,
        paths["archive_file_path"]
    )

    spark.stop()
    write_log("Spark session stopped")
    write_log("Pipeline completed successfully")


if __name__ == "__main__":
    main()