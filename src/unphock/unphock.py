# /usr/bin/env python3

"""Separate acquisitions into individual data files"""

import argparse
import datetime
import pytz
import pathlib
import numpy as np
import polars as pl
import untangle


TIMEZONE = pytz.timezone("America/Montreal")
PREFIXES = {
    "acc": "Accelerometer",
    "gyr": "Gyroscope",
    "loc": "Location",
    "mag": "Magnetometer",
}


def treat_xml(xdir: pathlib.Path, phone_id: str) -> dict[int, dict[str, pl.DataFrame]]:
    for file in xdir.glob("*phyphox"):
        containers, *event_times = parse_xml(file)

        dct_instruments = separate_containers(containers)
        df_instruments = make_dfs(dct_instruments)
        experiments = split_dfs(df_instruments, event_times)
        return experiments


def parse_xml(file: pathlib.Path) -> tuple[list[untangle.Element]]:
    unt = untangle.parse(str(file))
    containers = unt.phyphox.data_containers.children
    start_times, pause_times = unt.phyphox.events.start, unt.phyphox.events.pause

    return containers, start_times, pause_times


def separate_containers(
    containers: list[untangle.Element],
) -> dict[str, dict[str, np.ndarray]]:
    dct_init = {
        getattr(_l, "cdata"): np.array(
            tuple(map(float, _l._attributes["init"].split(",")))
        )
        for _l in containers
    }
    dct_sep = {
        prefix: {
            k: dct_init[k]
            for k in filter(lambda k: k[: len(prefix)] == prefix, dct_init.keys())
        }
        for prefix in PREFIXES
    }
    dct_sep["loc"].pop("locStatus")
    dct_sep["loc"].pop("locSatellites")
    return dct_sep


def parse_csv():
    pass


def make_dfs(
    dct_instruments: dict[str, dict[str, np.ndarray]]
) -> dict[str, pl.DataFrame]:
    return {k: pl.from_dict(v) for k, v in dct_instruments.items()}


def split_dfs(
    dct_dfs: dict[str, pl.DataFrame], event_times: tuple[list[untangle.Element]]
) -> dict[int, dict[str, pl.DataFrame]]:
    experiments = {}
    for i, times in enumerate(*event_times):
        experiments[i] = {}
        start_time, pause_time = [
            float(_e._attributes["experimenTime"]) for _e in times
        ]
        for key, df in dct_dfs.items():
            time_col = f"{key}_time"
            experiments[i][key] = (
                df.filter(
                    (pl.col(time_col) >= start_time) & (pl.col(time_col) < pause_time)
                )
                .with_columns(
                    (1e6 * pl.col(time_col).cast(pl.Duration)).alias(time_col)
                )
                .with_columns(
                    pl.col(time_col)
                    + datetime.datetime.fromtimestamp(
                        int(times[0]._attributes["systemTime"]) / 1000, TIMEZONE
                    )
                )
            )
    return experiments


def write_dfs(
    out_root: pathlib.Path,
    experiments: dict[int, dict[str, pl.DataFrame]],
    phone_id: str,
):
    phone_path = out_root.joinpath(phone_id)
    if not phone_path.exists():
        phone_path.mkdir()

    for exp_id, experiment in experiments.items():
        directory = phone_path.joinpath(f"T_{exp_id:04d}_{phone_id}_AGML")
        directory.mkdir(exist_ok=True)
        print(f"saving df to {directory}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=pathlib.Path)
    parser.add_argument("output_dir", type=pathlib.Path)

    args = parser.parse_args()

    in_root, out_root = args

    for path in in_root.iterdir():
        phone_id = path.stem
        if len(phone_id) in (1, 2):
            try:
                int(phone_id)
            except ValueError:
                continue

        phone_id = f"{int(phone_id):02d}"
        xml_dir = path.joinpath("XML")
        if xml_dir.exists():
            experiments = treat_xml(xml_dir, phone_id)
            write_dfs(out_root, experiments, phone_id)
        # meta_dir = path.joinpath("meta")
        # csv_files = path.glob("*.csv")
        # if meta_dir.exists():
        #     parse_csv(meta_dir, csv_files)


if __name__ == "__main__":
    main()
