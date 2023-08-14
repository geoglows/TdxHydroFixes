# TDXHydroFixes
Louis R. Rosas, Dr Riley Hales, Josh Ogden

## Overview
Provides functions for fixing errors in the TDX stream network and basins.

Reads in provided stream networks and catchments

Fixes stream segments of 0 length for different cases as follows:

Feature is coastal w/ no upstream or downstream
Delete the stream and its basin
Feature is bridging a 3-river confluence (Has downstream and upstreams)
Artificially create a basin with 0 area, and force a length on the point of 1 meter
Feature is costal w/ upstreams but no downstream
Force a length on the point of 1 meter
Feature doesn't match any previous case
Raise an error for now