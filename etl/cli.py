import argparse
import sys
import yaml
from pathlib import Path

from etl.pipeline import ETLPipeline
from etl.checkpoint import CheckpointManager


def main():
    parser = argparse.ArgumentParser(
        description="MySQL to DolphinDB ETL — run a sync job"
    )
    parser.add_argument(
        "-c", "--config",
        default=Path(__file__).resolve().parent.parent / "config" / "config.yaml",
        help="Path to config YAML (default: config/config.yaml)",
    )
    parser.add_argument(
        "-j", "--job",
        help="Job name to run (default: run all jobs)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List configured jobs and exit",
    )
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    if args.list:
        print("Configured jobs:")
        for j in config.get("jobs", []):
            print(f"  - {j['name']}: {j['mysql_table']} -> {j['dolphindb_table']}")
        return

    checkpoint_mgr = CheckpointManager(config["checkpoint"]["db_path"])
    jobs = config.get("jobs", [])

    if args.job:
        jobs = [j for j in jobs if j["name"] == args.job]
        if not jobs:
            print(f"No job named '{args.job}' found in config.")
            sys.exit(1)

    for job_conf in jobs:
        print(f"Running job: {job_conf['name']}")
        pipeline = ETLPipeline(
            job_conf,
            config["mysql"],
            config["dolphindb"],
            checkpoint_mgr,
        )
        pipeline.run()
        print(f"  checkpoint: {checkpoint_mgr.load(job_conf['name'])}")


if __name__ == "__main__":
    main()
