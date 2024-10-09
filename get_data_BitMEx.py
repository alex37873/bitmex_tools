import os
import sys

import pandas as pd
from settings import DOWNLOAD_FOLDER, OUTPUT_FOLDER

if __name__ == "__main__":
    TS_STEP = int(sys.argv[1]) if len(sys.argv) > 1 else 30

    files = os.listdir(DOWNLOAD_FOLDER)
    files = sorted([os.path.join(DOWNLOAD_FOLDER, n) for n in files])
    tmp = pd.read_csv(filepath_or_buffer=files[0])

    TARGET_SYMBOLS = [n for n in tmp.symbol.unique() if n.find("_") == -1]

    res = pd.DataFrame()

    del tmp

    for path in files:
        print(path)

        df = pd.read_csv(
            path,
            parse_dates=[0],
            date_format="%Y-%m-%dD%H:%M:%S.%f",
        )
        df["ts"] = df["timestamp"].astype(int) // 1000000000
        df["ts_idx"] = (df["ts"] // TS_STEP).astype(int)

        grouped = df.groupby(by=["ts_idx", "symbol"]).last()
        grouped.reset_index(inplace=True)

        out = pd.DataFrame(columns=["ts_idx"], dtype=int)

        for syb in TARGET_SYMBOLS:
            slice = grouped.loc[grouped["symbol"] == syb]

            tmp = pd.DataFrame()
            tmp["ts_idx"] = slice["ts_idx"] * TS_STEP
            tmp[f"{syb}_ask_px_1"] = slice["askPrice"]
            tmp[f"{syb}_ask_qt_1"] = slice["askSize"]
            tmp[f"{syb}_bid_px_1"] = slice["bidPrice"]
            tmp[f"{syb}_bid_qt_1"] = slice["bidSize"]

            out = out.merge(
                right=tmp, on="ts_idx", how="outer"
            )  # , lsuffix=f'_{syb}', rsuffix=f'_{syb}')

        for syb in TARGET_SYMBOLS:
            out[f"{syb}_tr_ask_px"] = 0.0
            out[f"{syb}_tr_ask_qt"] = 0.0
            out[f"{syb}_tr_bid_px"] = 0.0
            out[f"{syb}_tr_bid_qt"] = 0.0

        res = pd.concat(objs=[res, out], ignore_index=True)

    res.ffill(inplace=True)
    res.bfill(inplace=True)
    res.to_csv(path_or_buf=os.path.join(
        OUTPUT_FOLDER, "BitmexData.csv"), index=False)
