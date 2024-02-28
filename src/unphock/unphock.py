# /usr/bin/env python3

"""Separate acquisitions into individual data files"""

import argparse
import datetime
import functools
import operator
import pytz
import pathlib
import numpy as np
import polars as pl
import untangle
import warnings


TIMEZONE = pytz.timezone("America/Montreal")
PREFIXES = {
    "acc": "Accelerometer",
    "gyr": "Gyroscope",
    "loc": "Location",
    "mag": "Magnetometer",
}


def iterate(in_root: pathlib.Path, out_root: pathlib.Path, **kwargs):
    for path in in_root.iterdir():
        phone_id = path.stem
        if len(phone_id) in (1, 2):
            try:
                phone_id = f"{int(phone_id):02d}"
            except ValueError:
                warnings.warn(
                    f"Skipping {phone_id} that does not look like a phone ID",
                    stacklevel=1,
                )
                continue
        else:
            warnings.warn(
                f"Skipping {phone_id} that does not look like a phone ID", stacklevel=1
            )
            continue

        if kwargs["verbose"]:
            print("---")
            print(f"Treating phone {phone_id}")

        xml_dir = path.joinpath("XML")
        xml_experiments = treat_xml_dir(xml_dir, phone_id) if xml_dir.exists() else {}

        # TODO
        meta_time_file = path.joinpath("meta").join("time.csv")
        csv_files = path.glob("*.csv")
        csv_files = [file for file in csv_files if file.stem in PREFIXES.values()]
        if meta_time_file.exists() and len(csv_files) > 1:
            treat_csv_files(meta_time_file, csv_files)
        csv_experiments = {}

        experiments = xml_experiments | csv_experiments
        if len(experiments) > 1:
            write_dfs(out_root, experiments, phone_id, **kwargs)


def treat_xml_dir(
    xdir: pathlib.Path, phone_id: str
) -> dict[int, dict[str, pl.DataFrame]]:
    return functools.reduce(
        operator.ior, (treat_xml_file(file) for file in xdir.glob("*phyphox")), {}
    )


def treat_xml_file(file: pathlib.Path) -> dict[int, dict[str, pl.DataFrame]]:
    containers, exports, *event_times = parse_xml(file)
    dct_instruments = separate_containers(containers)
    headers = prettify_headers(exports)
    df_instruments = make_dfs(dct_instruments, headers)
    experiments = split_dfs(df_instruments, event_times)
    return experiments


def parse_xml(file: pathlib.Path) -> tuple[list[untangle.Element]]:
    unt = untangle.parse(str(file))
    containers = unt.phyphox.data_containers.children
    exports = unt.phyphox.export.children
    start_times, pause_times = unt.phyphox.events.start, unt.phyphox.events.pause

    return containers, exports, start_times, pause_times


def separate_containers(
    containers: list[untangle.Element],
) -> dict[str, dict[str, np.ndarray]]:
    dct_init = {
        _l.cdata: np.array(tuple(map(float, _l._attributes["init"].split(","))))
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


def prettify_headers(exports: list[untangle.Element]):
    return {
        child._attributes["name"]: {
            _e.cdata: _e._attributes["name"] for _c in child.children for _e in _c
        }
        for child in exports
    }


def parse_xml_time(
    event_times: tuple[list[untangle.Element]],
) -> dict[str, list[tuple[float], tuple[int]]]:
    dct = {"START": None, "PAUSE": None}
    events_flat = (_ee for _e in event_times for _ee in _e)
    for k in dct:
        dct[k] = [
            tuple(
                float(_e._attributes["experimentTime"])
                for _e in filter(lambda e: e._name.upper() == k, events_flat)
            ),
            tuple(
                int(_e._attributes["systemTime"])
                for _e in filter(lambda e: e._name.upper() == k, events_flat)
            ),
        ]
    return dct


def treat_csv_files(meta_time_file: pathlib.Path, csv_files: list[pathlib.Path]):
    meta_times = parse_meta_time(meta_time_file)
    parse_csv(meta_times, csv_files)


def parse_meta_time(file: pathlib.Path) -> dict[str, list[tuple[float], tuple[int]]]:
    df = pl.read_csv(file, dtypes={"system time": str})
    dct = {
        event: df.filter(pl.col("event") == event)
        .select(pl.col("experiment time"), pl.col("system time"))
        .to_numpy()
        .T
        for event in ("START", "PAUSE")
    }
    for k in dct:
        dct[k][0] = tuple(dct[k][0].astype(float))
    for k in dct:
        dct[k][1] = tuple(
            map(
                lambda t: int(t[0]) * 1000 + int(t[1]),
                map(lambda s: s.split("."), dct[k][1]),
            )
        )
    return dct


def parse_csv(
    meta_times: dict[str, list[tuple[float | int]]], csv_files: list[pathlib.Path]
):
    for file in csv_files:
        pl.read_csv(file)


def make_dfs(
    dct_instruments: dict[str, dict[str, np.ndarray]],
    exports: dict[str, dict[str, str]],
) -> dict[str, pl.DataFrame]:
    dfs = {k: pl.from_dict(v) for k, v in dct_instruments.items()}
    for k, df in dfs.items():
        dfs[k] = df.select(
            (pl.col(k).alias(v) for k, v in exports[PREFIXES[k]].items())
        )
    return dfs


def split_dfs(
    dct_dfs: dict[str, pl.DataFrame], event_times: tuple[list[untangle.Element]]
) -> dict[int, dict[str, pl.DataFrame]]:
    experiments = {}
    for times in zip(*event_times):
        start_time, pause_time = [
            float(_e._attributes["experimentTime"]) for _e in times
        ]
        start_timestamp = int(times[0]._attributes["systemTime"])
        experiments[start_timestamp] = {}
        for key, df in dct_dfs.items():
            # time_col = f"{key}_time"
            time_col = "Time (s)"
            experiments[start_timestamp][key] = (
                df.filter(
                    (pl.col(time_col) >= start_time) & (pl.col(time_col) < pause_time)
                )
                # .with_columns(
                #     (1e6 * pl.col(time_col).cast(pl.Duration)).alias(time_col)
                # )
                .with_columns(
                    (
                        (1e6 * pl.col(time_col)).cast(pl.Duration)
                        + datetime.datetime.fromtimestamp(
                            start_timestamp / 1000, TIMEZONE
                        )
                    ).alias("local_time")
                )
            )
    return experiments


def write_dfs(
    out_root: pathlib.Path,
    experiments: dict[int, dict[str, pl.DataFrame]],
    phone_id: str,
    **kwargs,
):
    phone_path = out_root.joinpath(phone_id)
    if not phone_path.exists():
        if kwargs["verbose"]:
            print("Directory {phone_path} does not exist, creating directory")
        phone_path.mkdir()

    for exp_id, experiment in enumerate(experiments.values()):
        directory = phone_path.joinpath(f"T_{exp_id+1:04d}_{phone_id}_AGML")
        directory.mkdir(exist_ok=True)
        for k, v in PREFIXES.items():
            file = directory.joinpath(f"{v}.csv")
            if kwargs["verbose"]:
                print(f"Saving {k} data to {file}")
            if not kwargs["dry_run"]:
                experiment[k].write_csv(file)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=pathlib.Path)
    parser.add_argument("output_dir", type=pathlib.Path)
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="TODO",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="TODO",
    )

    args = parser.parse_args()
    kwargs = {k: getattr(args, k) for k in ("dry_run", "verbose")}

    iterate(args.input_dir, args.output_dir, **kwargs)


if __name__ == "__main__":
    main()
