import logging
import sys
import glob
import os
import traceback

from tdxhydrofixes.inputs import stream_corrections
from tdxhydrofixes.network import correct_0_length_basins

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)

inputs_path = '/Users/ricky/Downloads/test_tdxhydro'
outputs_path = '/Users/ricky/Downloads/test_output'
regions_to_select = '*'

gis_iterable = zip(
    sorted(glob.glob(os.path.join(inputs_path, f'TDX_streamnet_{regions_to_select}.gpkg')), reverse=False),
    sorted(glob.glob(os.path.join(inputs_path, f'TDX_streamreach_basins_{regions_to_select}.gpkg')), reverse=False),
)

id_field = 'LINKNO'
basin_id_field = 'streamID'
ds_field = 'DSLINKNO'
order_field = 'strmOrder'
length_field = 'Length'

if __name__ == '__main__':
    for streams_gpq, basins_gpq in gis_iterable:
        # Identify the region being processed
        region_num = os.path.basename(streams_gpq)
        region_num = region_num.split('_')[2]
        region_num = int(region_num)

        # log a bunch of stuff
        logging.info('')
        logging.info(region_num)
        logging.info(streams_gpq)
        logging.info(basins_gpq)

        # output names
        out_streams = os.path.join(outputs_path, os.path.basename(streams_gpq))
        out_basins = os.path.join(outputs_path, os.path.basename(basins_gpq))

        try:
            # streams
            if not os.path.exists(out_streams):
                steams_gdf = stream_corrections(streams_gpq,
                                    save_dir=outputs_path,
                                    id_field=id_field,
                                    ds_id_field=ds_field,
                                    length_field=length_field,
                                    region_num=region_num
                                    )               
                logging.info('\tWriting Output Streams')
                steams_gdf.to_file(out_streams, driver='GPKG')

            # basins
            if not os.path.exists(out_basins):
                logging.info('Reading basins')
                basins_gdf = correct_0_length_basins(basins_gpq,
                                        save_dir=outputs_path,
                                        stream_id_col=basin_id_field,
                                        region_num=region_num
                                        )
                logging.info('\tWriting Output Basins')
                basins_gdf.to_file(out_basins, driver='GPKG')
                
        except Exception as e:
            logging.info('\n----- ERROR -----\n')
            logging.info(e)
            logging.error(traceback.format_exc())
            continue

    logging.info('All TDX Hydro Regions Processed')