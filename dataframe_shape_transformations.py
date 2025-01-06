import pandas as pd
import numpy as np


def transform_long_to_wide(df, group_id='group_id', time_id='time_id', resolution='15T'):
    """
    generate time ids if not specified,
    transform a long dataframe into a wide dataframe
    :param df: pandas dataframe
                columns: group_id, features, targets
                        datetime index or time_id column (one of them)
    :param group_id: column to use to make new frame’s index
    :param time_id: column to use to make new frame’s columns
            the rest of column(s) are features and targets, would be used for populating new frame’s values.
    :return: df, pandas dataframe, group id as index, (feat,time_id) as columns
    """
    df_copy = df.copy()

    if time_id not in df_copy.columns: # generate column ids based on the datetime index
        assert isinstance(df_copy, pd.DatetimeIndex), 'set time_id or use DatetimeIndex!'
        df_copy[time_id] = df_copy.index.to_series().apply(lambda x: pd.Timedelta(hours=x.hour, minutes=x.minute)/pd.Timedelta(resolution) )

    feat_tar_cols = [col for col in df_copy.columns if col not in [group_id, time_id]]
    wide_df = df_copy.pivot(index=group_id, columns=time_id, values=feat_tar_cols)
    return wide_df


def transform_wide_to_long(wide_df):
    """
    transform a wide dataframe into a long dataframe
    :param df: pandas dataframe with multilevel columns (feat,time_id)
    :return: df, pandas dataframe, with int index, long format, columns: group_id, time_id, features and targets
    """
    long_df = wide_df.stack(1).reset_index()
    return long_df


def transform_long_to_multiindex(df, group_id='group_id', time_id='time_id', resolution='15T'):
    """
    generate time ids if not specified,
    transform a long dataframe into a wide dataframe with (group_id,feature+tar) index, time_id as columns
    :param df: pandas dataframe
                columns: group_id, features, targets
                        datetime index or time_id column (one of them)
    :param group_id: column to use to make new frame’s index
    :param time_id: column to use to make new frame’s columns
            the rest of column(s) are features and targets, would be used for populating new frame’s values.
    :return: df, pandas dataframe, (group id,feat) as index, time_id as columns
    """
    df_copy = df.copy()

    if time_id not in df_copy.columns: # generate column ids based on the datetime index, TODO: based on the given resolution
        assert isinstance(df_copy, pd.DatetimeIndex), 'set time_id or use DatetimeIndex!'
        df_copy[time_id] = df_copy.index.to_series().apply(lambda x: pd.Timedelta(hours=x.hour, minutes=x.minute)/pd.Timedelta(resolution) )

    group_ids = df_copy[group_id].unique()
    feat_tar_cols = [col for col in df_copy.columns if col not in [group_id, time_id]]
    multiindex_df = pd.concat( [df_copy.loc[df_copy[group_id]==grp_id, feat_tar_cols+[time_id]].set_index(time_id).T.set_index( \
        pd.MultiIndex.from_product([[grp_id], feat_tar_cols], names=[group_id, "channels"])) for grp_id in group_ids] )    #TODO: accelerate this part

    return multiindex_df


def transform_multiindex_to_long(df, group_id='group_id', time_id='time_id'):
    """
    transpose dataframe to make time_id columns as index, features as columns
    :param df:
    :param time_id:
    :return:
    """
    df_copy = df.copy()
    group_ids = df_copy.index.get_level_values(0).unique()

    df_concat = []
    for grp_id in group_ids:
        grp_df = df_copy.loc[grp_id].T.copy()
        grp_df[group_id] = grp_id
        grp_df.index.name = time_id
        grp_df = grp_df.reset_index()
        grp_df = grp_df.rename_axis(None, axis=1)
        df_concat.append(grp_df)

    return pd.concat(df_concat).reset_index(drop=True)
