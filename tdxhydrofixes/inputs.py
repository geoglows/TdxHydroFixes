import logging
import os

import geopandas as gpd
import pandas as pd

# set up logging
logger = logging.getLogger(__name__)

from .network import correct_0_length_streams
from .network import identify_0_length

__all__ = [
    'stream_corrections',
]

def stream_corrections(streams_gpq: str,
                save_dir: str,
                id_field: str = 'LINKNO',
                ds_id_field: str = 'DSLINKNO',
                length_field: str = 'Length', 
                region_num: int = 0) -> gpd.GeoDataFrame:
    """
    Correct stream network, prep files for basin corrections

    Saves the following files to the save_dir:
        - rapid_inputs_master.parquet
        - {region_num}_dissolved_network.gpkg
        - mod_zero_length_streams.csv (if any 0 length streams are found)
        - mod_basin_zero_centroid.csv (if any basins have an ID of 0 and geometry is not available)
        - mod_drop_small_streams.csv (if drop_small_watersheds is True)
        - mod_dissolved_headwaters.csv (if dissolve_headwaters is True)
        - mod_pruned_branches.csv (if prune_branches_from_main_stems is True)


    Args:
        streams_gpq: str, path to the streams geoparquet
        save_dir: str, path to the directory to save the master files
        id_field: str, field name for the link id
        ds_id_field: str, field name for the downstream link id
        length_field: str, field name for the length of the stream segment
        default_k: float, default velocity factor (k) for Muskingum routing
        default_x: float, default attenuation factor (x) for Muskingum routing

        drop_small_watersheds: bool, drop small watersheds
        dissolve_headwaters: bool, dissolve headwater branches
        prune_branches_from_main_stems: bool, prune branches from main stems
        cache_geometry: bool, save the dissolved geometry as a geoparquet
        min_drainage_area_m2: float, minimum drainage area in m2 to keep a watershed
        min_headwater_stream_order: int, minimum stream order to keep a headwater branch

    Returns:
        None
    """
    sgdf = gpd.read_file(streams_gpq)

    logger.info('\tRemoving 0 length segments')
    if 0 in sgdf[length_field].values:
        zero_length_fixes_df = identify_0_length(sgdf, id_field, ds_id_field, length_field)
        zero_length_fixes_df.to_csv(os.path.join(save_dir, f'mod_zero_length_streams_{region_num}.csv'), index=False)
        sgdf = correct_0_length_streams(sgdf, zero_length_fixes_df, id_field, ds_id_field)

    # Fix basins with ID of 0
    if 0 in sgdf[id_field].values:
        logger.info('\Found basins with ID of 0')
        pd.DataFrame({
            id_field: [0, ],
            'centroid_x': sgdf[sgdf[id_field] == 0].centroid.x.values[0],
            'centroid_y': sgdf[sgdf[id_field] == 0].centroid.y.values[0]
        }).to_csv(os.path.join(save_dir, f'mod_basin_zero_centroid_{region_num}.csv'), index=False)

    return sgdf
