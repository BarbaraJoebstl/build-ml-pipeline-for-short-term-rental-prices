#!/usr/bin/env python
"""
input_artifact, output_artifact, output_type, output_description, min_price, max_price
"""

import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    run = wandb.init(job_type="basic data cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("loading raw data")
    artifact_local_path = run.use_artifact(f"{args.input_artifact}").file()
    df = pd.read_csv(artifact_local_path)
    logger.info("Perfom data cleaning")
    # Drop outliers
    idx = df["price"].between(args.min_price, args.max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df["last_review"] = pd.to_datetime(df["last_review"])
    logger.info("Store cleaned data to wandb")
    df.to_csv(args.output_artifact, index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="download raw csv, perform cleaning and upload the cleaned csv to wandb"
    )

    parser.add_argument(
        "--input_artifact",
        default="sample.csv:latest",
        type=str,
        help="Name of the input artifact, raw dataset CSV",
        required=True,
    )

    parser.add_argument(
        "--output_artifact",
        default="clean_sample.csv",
        type=str,
        help="Name for the output artifact to save the cleaned dataset",
        required=True,
    )

    parser.add_argument(
        "--output_type",
        default="clean_sample",
        type=str,
        help="Type of the output artifact, e.g., 'dataset'",
        required=True,
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Short description of the cleaned dataset (e.g., outliers removed, price filtered)",
        required=True,
    )

    parser.add_argument(
        "--min_price",
        default=10.00,
        type=float,
        help="Minimum acceptable price for listings to keep in the dataset",
        required=True,
    )

    parser.add_argument(
        "--max_price",
        default=350.00,
        type=float,
        help="Maximum acceptable price for listings to keep in the dataset",
        required=True,
    )

    args = parser.parse_args()

    go(args)
