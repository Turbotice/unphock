# /usr/bin/env python3

import datetime
import pytz
import pathlib
import numpy as np
import polars as pl
import untangle


root_raw = pathlib.Path(
    "/run/user/1896521/gvfs/smb-share:server=saguenay.local,share=share_hublot/Data/0226/Telephones/raw"
)
root_dest = "/run/user/1896521/gvfs/smb-share:server=saguenay.local,share=share_hublot/Data/0226/Telephones"

TIMEZONE = pytz.timezone("America/Montreal")
PREFIXES = {
    "acc": "Accelerometer",
    "gyr": "Gyroscope",
    "loc": "Location",
    "mag": "Magnetometer",
}


test_path = root.joinpath("04/XML/1418.phyphox")


def iterate_dirs(in_root: pathlib.Path, out_root: pathlib.Path):
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
        for key, df in dct_dfs.items():
            time_col = f"{key}_time"
            experiments[i][key] = df.filter(
                (pl.col() >= times[0]) & (pl.col(time_col) < times[1])
            )
    return experiments


def write_dfs(
    out_root: pathlib.path,
    experiments: dict[int, dict[str, pl.DataFrame]],
    phone_id: str,
):
    phone_path = out_root.joinpath(phone_id)
    if not phone_path.exists():
        phone_path.mkdir()

    for exp_id, experiment in experiments.items():

        directory = phone_path.joinpath(f"T_{exp_id:04d}_{phone_id}_")
        directory.mkdir
    pass


_d = datetime.datetime.fromtimestamp(
    1708979062356 / 1000, pytz.timezone("America/Montreal")
)
