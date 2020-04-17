from pandas import DataFrame, concat


def remove_fractions(df, colname):
    df[colname] = df[colname].astype(str)
    df[colname] = df[colname].str.split('.').str[0]


def drop_columns_except(df, columns_to_keep):
    # Remove duplicate columns
    df = df.loc[:,~df.columns.duplicated()]
    # Drop unwanted columns.
    return df.reindex(columns=columns_to_keep)


def drop_rows_with_col_word(df, columnname, word):
    # Drop unwanted rows
    pattern = r'\b%s\b' % word
    df = df[~df[columnname].str.contains(pattern, case=False)]
    return df


def lookup_values_by_key(df, lookupcolumn, key, valuecolumn):
    return df.query('%s==%s' % (lookupcolumn, key))[valuecolumn]


def hxlate(df, hxl_names):
    hxl_columns = [hxl_names[c] for c in df.columns]
    hxl = DataFrame.from_records([hxl_columns], columns=df.columns)
    df = concat([hxl, df])
    df.reset_index(inplace=True, drop=True)
    return df


def remove_nonenan(df, colname):
    df[colname] = df[colname].astype(str)
    df[colname].replace(['nan', 'none'], ['', ''], inplace=True)
