from pipeline.logger import write_log

from pipeline.file_manager import (
    get_latest_incoming_file,
    prepare_file_paths,
    copy_to_raw,
    archive_incoming_file,
)

from pipeline.spark_utils import create_spark_session

from pipeline.transform import (
    standardize_column_names,
    convert_data_types,
    add_features,
)

from pipeline.validate import (
    apply_business_rules,
    split_valid_rejected,
)

from pipeline.azure_blob import (
    upload_file,
    upload_folder,
)

from pipeline.azure_sql import (
    create_retail_sales_table,
    load_parquet_to_retail_sales,
)


def main():
    spark = None

    try:
        write_log("Pipeline started")

        incoming_file_path = get_latest_incoming_file()
        paths = prepare_file_paths(incoming_file_path)

        copy_to_raw(
            incoming_file_path,
            paths["raw_file_path"],
        )

        write_log(
            f"Incoming file copied to raw folder: "
            f"{paths['raw_file_path']}"
        )

        upload_file(
            container_name="raw",
            local_file_path=paths["raw_file_path"],
            blob_name=paths["file_name"],
        )

        write_log(
            f"Raw file uploaded to Azure Blob Storage: "
            f"raw/{paths['file_name']}"
        )

        spark = create_spark_session()
        write_log("Spark session started")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("encoding", "ISO-8859-1")
            .csv(paths["raw_file_path"])
        )

        write_log(
            f"Raw file loaded into Spark: "
            f"{paths['raw_file_path']}"
        )

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
        write_log(f"Output folder: {paths['folder_name']}")
        write_log(f"Total records: {total_records}")
        write_log(f"Valid records: {valid_records}")
        write_log(f"Rejected records: {rejected_records}")

        print("\nSample rejected records:")

        rejected_df.select(
            "row_id",
            "order_id",
            "quantity",
            "discount",
            "sales",
            "order_date",
            "ship_date",
            "rejection_reason",
        ).show(10, truncate=False)

        valid_df.write.mode("overwrite").parquet(
            paths["processed_output_path"]
        )

        write_log(
            f"Valid processed data saved to: "
            f"{paths['processed_output_path']}"
        )

        rejected_df.write.mode("overwrite").parquet(
            paths["rejected_output_path"]
        )

        write_log(
            f"Rejected data saved to: "
            f"{paths['rejected_output_path']}"
        )

        upload_folder(
            container_name="processed",
            local_folder_path=paths["processed_output_path"],
            blob_folder_name=paths["folder_name"],
        )

        write_log(
            f"Processed data uploaded to Azure: "
            f"processed/{paths['folder_name']}"
        )

        upload_folder(
            container_name="rejected",
            local_folder_path=paths["rejected_output_path"],
            blob_folder_name=paths["folder_name"],
        )

        write_log(
            f"Rejected data uploaded to Azure: "
            f"rejected/{paths['folder_name']}"
        )

        create_retail_sales_table()
        write_log("Azure SQL table dbo.retail_sales is ready")

        load_parquet_to_retail_sales(
            paths["processed_output_path"]
        )

        write_log(
            f"Processed data loaded into Azure SQL from: "
            f"{paths['processed_output_path']}"
        )

        archive_incoming_file(
            incoming_file_path,
            paths["archive_file_path"],
        )

        write_log(
            f"Incoming file archived locally: "
            f"{paths['archive_file_path']}"
        )

        upload_file(
            container_name="archive",
            local_file_path=paths["archive_file_path"],
            blob_name=paths["file_name"],
        )

        write_log(
            f"Archived file uploaded to Azure: "
            f"archive/{paths['file_name']}"
        )

        write_log("Pipeline completed successfully")

    except Exception as error:
        write_log(
            f"Pipeline failed: {type(error).__name__}: {error}"
        )
        raise

    finally:
        if spark is not None:
            spark.stop()
            write_log("Spark session stopped")

        try:
            upload_file(
                container_name="logs",
                local_file_path="data/logs/pipeline_log.txt",
                blob_name="pipeline_log.txt",
            )

            write_log("Pipeline log uploaded to Azure")

        except Exception as log_upload_error:
            print(
                "Pipeline log upload failed:",
                log_upload_error,
            )


if __name__ == "__main__":
    main()