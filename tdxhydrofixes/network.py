import glob
import logging
import os

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import shapely.geometry as sg

__all__ = [
    'identify_0_length',
    'correct_0_length_streams',
    'correct_0_length_basins',
]

logger = logging.getLogger(__name__)

def identify_0_length(gdf: gpd.GeoDataFrame,
                      stream_id_col: str,
                      ds_id_col: str,
                      length_col: str, ) -> pd.DataFrame:
    """
    Fix streams that have 0 length.
    General Error Cases:
    1) Feature is coastal w/ no upstream or downstream
        -> Delete the stream and its basin
    2) Feature is bridging a 3-river confluence (Has downstream and upstreams)
        -> Artificially create a basin with 0 area, and force a length on the point of 1 meter
    3) Feature is costal w/ upstreams but no downstream
        -> Force a length on the point of 1 meter
    4) Feature doesn't match any previous case
        -> Raise an error for now

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Stream network
    stream_id_col : string
        Field in stream network that corresponds to the unique id of each stream segment
    ds_id_col : string
        Field in stream network that corresponds to the unique downstream id of each stream segment
    length_col : string
        Field in basins network that corresponds to the unique length of each stream segment
    """
    case1_ids = []
    case2_ids = []
    case3_ids = []
    case4_ids = []

    for rivid in gdf[gdf[length_col] == 0][stream_id_col].values:
        feat = gdf[gdf[stream_id_col] == rivid]

        # Case 1
        if feat[ds_id_col].values == -1 and feat['USLINKNO1'].values == -1 and feat['USLINKNO2'].values == -1:
            case1_ids.append(rivid)

        # Case 2
        elif feat[ds_id_col].values != -1 and feat['USLINKNO1'].values != -1 and feat['USLINKNO2'].values != -1:
            case2_ids.append(rivid)

        # Case 3
        elif feat[ds_id_col].values == -1 and feat['USLINKNO1'].values != -1 and feat['USLINKNO2'].values != -1:
            case3_ids.append(rivid)

        # Case 4
        else:
            logging.warning(f"The stream segment {feat[stream_id_col]} has conditions we've not yet considered")
            case4_ids.append(rivid)

    # variable length lists with np.nan to make them the same length
    longest_list = max([len(case1_ids), len(case2_ids), len(case3_ids), len(case4_ids), ])
    case1_ids = case1_ids + [np.nan] * (longest_list - len(case1_ids))
    case2_ids = case2_ids + [np.nan] * (longest_list - len(case2_ids))
    case3_ids = case3_ids + [np.nan] * (longest_list - len(case3_ids))
    case4_ids = case4_ids + [np.nan] * (longest_list - len(case4_ids))

    return pd.DataFrame({
        'case1': case1_ids,
        'case2': case2_ids,
        'case3': case3_ids,
        'case4': case4_ids,
    })

def correct_0_length_streams(sgdf: gpd.GeoDataFrame,
                             zero_length_df: pd.DataFrame,
                             id_field: str,
                             ds_id_field: str, ) -> gpd.GeoDataFrame:
    """
    Apply fixes to streams that have 0 length.

    Args:
        sgdf:
        zero_length_df:
        id_field:

    Returns:

    """
    # Case 1 - Coastal w/ no upstream or downstream - Delete the stream and its basin
    c1 = zero_length_df['case1'].dropna().astype(int).values
    sgdf = sgdf[~sgdf[id_field].isin(c1)]

    # Case 3 - Coastal w/ upstreams but no downstream - Assign small non-zero length
    # Apply before case 2 to handle some edges cases where zero length basins drain into other zero length basins
    c3_us_ids = sgdf[sgdf[id_field].isin(zero_length_df['case3'].dropna().values)][
        ['USLINKNO1', 'USLINKNO2']].values.flatten()
    sgdf.loc[sgdf[id_field].isin(c3_us_ids), ds_id_field] = -1
    sgdf = sgdf[~sgdf[id_field].isin(zero_length_df['case3'].dropna().values)]

    # Case 2 - Allow 3-river confluence - Delete the temporary basin and modify the connectivity properties
    # Sort by DSLINKNO to handle some edges cases where zero length basins drain into other zero length basins
    c2 = sgdf[sgdf[id_field].isin(zero_length_df['case2'].dropna().astype(int).values)]
    c2 = c2.sort_values(by=[ds_id_field], ascending=True)
    c2 = c2[id_field].values
    for river_id in c2:
        ids_to_apply = sgdf.loc[sgdf[id_field] == river_id, ['USLINKNO1', 'USLINKNO2', ds_id_field]]
        # if the downstream basin is also a zero length basin, find the basin 1 step further downstream
        if ids_to_apply[ds_id_field].values[0] in c2:
            ids_to_apply[ds_id_field] = \
                sgdf.loc[sgdf[id_field] == ids_to_apply[ds_id_field].values[0], ds_id_field].values[0]
        sgdf.loc[
            sgdf[id_field].isin(ids_to_apply[['USLINKNO1', 'USLINKNO2']].values.flatten()), ds_id_field] = \
            ids_to_apply[ds_id_field].values[0]

    # Remove the rows corresponding to the rivers to be deleted
    sgdf = sgdf[~sgdf[id_field].isin(c2)]

    return sgdf

def correct_0_length_basins(basins_gpq: str,
                            save_dir: str,
                            stream_id_col: str, 
                            region_num: int) -> gpd.GeoDataFrame:
    """
    Apply fixes to streams that have 0 length.

    Args:
        basins_gpq: Basins to correct
        save_dir: Directory to save the corrected basins to
        stream_id_col:

    Returns:

    """
    basin_gdf = gpd.read_file(basins_gpq)

    zero_fix_csv_path = os.path.join(save_dir, f'mod_basin_zero_centroid_{region_num}.csv')
    if os.path.exists(zero_fix_csv_path):
        box_radius_degrees = 0.015
        basin_zero_centroid = pd.read_csv(zero_fix_csv_path)
        centroid_x = basin_zero_centroid['centroid_x'].values[0]
        centroid_y = basin_zero_centroid['centroid_y'].values[0]
        link_zero_box = gpd.GeoDataFrame({
            'geometry': [sg.box(
                centroid_x - box_radius_degrees,
                centroid_y - box_radius_degrees,
                centroid_x + box_radius_degrees,
                centroid_y + box_radius_degrees
            )],
            stream_id_col: [0, ]
        }, crs=basin_gdf.crs)
        basin_gdf = pd.concat([basin_gdf, link_zero_box])

    zero_length_csv_path = os.path.join(save_dir, f'mod_zero_length_streams_{region_num}.csv')
    if os.path.exists(zero_length_csv_path):
        logger.info('\tRevising basins with 0 length streams')
        zero_length_df = pd.read_csv(zero_length_csv_path)
        # Case 1 - Coastal w/ no upstream or downstream - Delete the stream and its basin
        logger.info('\tHandling Case 1 0 Length Streams - delete basins')
        basin_gdf = basin_gdf[~basin_gdf[stream_id_col].isin(zero_length_df['case1'])]
        # Case 2 - Allow 3-river confluence - basin does not exist (try to delete just in case)
        logger.info('\tHandling Case 2 0 Length Streams - delete basins')
        basin_gdf = basin_gdf[~basin_gdf[stream_id_col].isin(zero_length_df['case2'])]
        # Case 3 - Coastal w/ upstreams but no downstream - basin exists so delete it
        logger.info('\tHandling Case 3 0 Length Streams - delete basins')
        basin_gdf = basin_gdf[~basin_gdf[stream_id_col].isin(zero_length_df['case3'])]

    return basin_gdf.reset_index(drop=True)
