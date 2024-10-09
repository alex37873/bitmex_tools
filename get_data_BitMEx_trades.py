import os
import sys

import pandas as pd
from settings import DOWNLOAD_FOLDER, OUTPUT_FOLDER

if __name__ == "__main__":
    TS_STEP = int(sys.argv[1]) if len(sys.argv) > 1 else 30

    files = os.listdir(path=DOWNLOAD_FOLDER)
    files = sorted([os.path.join(DOWNLOAD_FOLDER, n) for n in files])
    tmp = pd.read_csv(filepath_or_buffer=files[0])

    TARGET_SYMBOLS = [n for n in tmp.symbol.unique() if n.find("_") == -1]

    res = pd.DataFrame()

    del tmp

    for path in files:
        print(path)

        df = pd.read_csv(
            filepath_or_buffer=path,
            parse_dates=[0],
            date_format="%Y-%m-%dD%H:%M:%S.%f",
        )
        df["ts"] = df["timestamp"].astype(int) // 1000000000
        df["ts_idx"] = (df["ts"] // TS_STEP).astype(int)

        grouped = df.groupby(by=["ts_idx", "symbol", "side"]).agg(
            {
                "price": ["last", "max", "min"],
                "size": "sum",
                "foreignNotional": "sum",
                "size": "sum",
            }
        )

        grouped.reset_index(inplace=True)

        out = grouped.pivot(index=["ts_idx"], columns=["symbol", "side"])
        out.columns = out.columns.map(mapper="_".join).str.strip(to_strip="_")
        out["ts"] = grouped["ts_idx"].unique() * TS_STEP
        res = pd.concat(objs=[res, out], ignore_index=False)

    res.fillna(value=0.0)
    res.to_csv(
        path_or_buf=os.path.join(OUTPUT_FOLDER, "BitmexData_30s_tr.csv"), index=False
    )
