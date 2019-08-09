import pandas as pd


def format_time(seconds):
    if seconds < 0.8:
        return "{:.0f}ms".format(seconds * 1000)
    if seconds < 60:
        return "{:.1f}s".format(seconds)
    if seconds < 3600:
        return "{:.1f}m".format(seconds / 60)
    return "{:.1f}h".format(seconds / 3600)


def unpack_frame(frame, unpack_column="config"):
    assert unpack_column in frame.columns

    keys = set()
    for item in frame[unpack_column]:
        assert isinstance(item, dict)
        keys.update(item.keys())

    for key in keys:
        assert key not in frame.columns

    columns = list(frame.columns)
    columns.remove(unpack_column)

    orig_columns = list(columns)
    columns += keys
    unpacked = []

    for (_, row) in frame.iterrows():
        unpack_val = row[unpack_column]
        row_value = {}
        for orig_col in orig_columns:
            row_value[orig_col] = row[orig_col]
        for key in keys:
            row_value[key] = unpack_val.get(key, pd.np.nan)
        unpacked.append(row_value)
    return pd.DataFrame(unpacked, columns=columns)
