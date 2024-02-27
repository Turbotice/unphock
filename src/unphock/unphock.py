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
tz = pytz.timezone("America/Montreal")


test_path = root.joinpath("04/XML/1418.phyphox")


def iterate_dirs():
    pass


def parse_xml():
    pass


def parse_csv():
    pass


def make_dfs():
    pass


def split_dfs():
    dct_df["acc"].filter(
        pl.col("acc_time") < float(starts[1]._attributes["experimentTime"])
    )
    dct_df["acc"].filter(
        pl.col("acc_time") >= float(starts[1]._attributes["experimentTime"])
    )
    pass


def write_dfs():
    pass


unt = untangle.parse(str(test_path))

prefixes = ("acc", "gyr", "loc", "mag")
containers = unt.phyphox.data_containers.children

dct_test = {
    getattr(_l, "cdata"): np.array(tuple(map(float, _l._attributes["init"].split(","))))
    for _l in containers
}

dct_sep = {
    prefix: {
        k: dct_test[k]
        for k in filter(lambda k: k[: len(prefix)] == prefix, dct_test.keys())
    }
    for prefix in prefixes
}
dct_sep["loc"].pop("locStatus")
dct_sep["loc"].pop("locSatellites")

dct_df = {k: pl.from_dict(v) for k, v in dct_sep.items()}

starts = unt.phyphox.events.start
starts


_d = datetime.datetime.fromtimestamp(
    1708979062356 / 1000, pytz.timezone("America/Montreal")
)
