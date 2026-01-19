#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""

import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    run = wandb.init(project="nyc_airbnb", job_type="data_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Downloading artifact")
    artifact = run.use_artifact(args.input_artifact)
    artifact_path = artifact.file()
    df = pd.read_csv(artifact_path)

    logger.info("execute data cleaning")
    # transform object to datetype
    df["last_review"] = pd.to_datetime(df["last_review"])

    # remove price outliers
    idx = df["price"].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # drop columns with high correlation (number of reviews, neighbourhodd group)
    df.drop(columns=["reviews_per_month", "latitude", "longitude"], inplace=True)

    logger.info("store cleaned artifact as csv")
    # store cleaned artifact
    df.to_csv("clean_sample.csv", index=False)
    # upload to W&B
    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument("--input_artifact", type=str, help="artifact to use from W&B must be a csv", required=True)

    parser.add_argument("--output_artifact", type=str, help="name of cleaned artifact", required=True)

    parser.add_argument("--output_type", type=str, help="type of the output", required=True)

    parser.add_argument("--output_description", type=str, help="cleaned version of the raw data", required=True)

    parser.add_argument("--min_price", type=int, help="min price for the housing", required=True)

    parser.add_argument("--max_price", type=int, help="max price for the housing", required=True)

    args = parser.parse_args()

    go(args)
