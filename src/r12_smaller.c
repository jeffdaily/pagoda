#include <stdio.h>
#include <stdlib.h>
#include <netcdf.h>

void
check_err(const int stat, const int line, const char *file)
{
    if (stat != NC_NOERR) {
        (void) fprintf(stderr, "line %d of %s: %s\n", line, file, nc_strerror(stat));
        exit(1);
    }
}

int
main()              /* create r12_smaller.nc */
{

    int  stat;           /* return status */
    int  ncid;           /* netCDF id */

    /* dimension ids */
    int time_dim;
    int cells_dim;
    int edges_dim;
    int layers_dim;

    /* dimension lengths */
    size_t time_len = NC_UNLIMITED;
    size_t cells_len = 167772162;
    size_t edges_len = 503316480;
    size_t layers_len = 3;

    /* variable ids */
    int grid_center_lat_id;
    int grid_center_lon_id;
    int grid_edge_lat_id;
    int grid_edge_lon_id;
    int edge_var_id;

    /* rank (number of dimensions) for each variable */
#  define RANK_grid_center_lat 1
#  define RANK_grid_center_lon 1
#  define RANK_grid_edge_lat 1
#  define RANK_grid_edge_lon 1
#  define RANK_edge_var 3

    /* variable shapes */
    int grid_center_lat_dims[RANK_grid_center_lat];
    int grid_center_lon_dims[RANK_grid_center_lon];
    int grid_edge_lat_dims[RANK_grid_edge_lat];
    int grid_edge_lon_dims[RANK_grid_edge_lon];
    int edge_var_dims[RANK_edge_var];

    /* enter define mode */
    stat = nc_create("r12_smaller.nc", NC_CLOBBER|NC_64BIT_OFFSET, &ncid);
    check_err(stat,__LINE__,__FILE__);

    /* define dimensions */
    stat = nc_def_dim(ncid, "time", time_len, &time_dim);
    check_err(stat,__LINE__,__FILE__);
    stat = nc_def_dim(ncid, "cells", cells_len, &cells_dim);
    check_err(stat,__LINE__,__FILE__);
    stat = nc_def_dim(ncid, "edges", edges_len, &edges_dim);
    check_err(stat,__LINE__,__FILE__);
    stat = nc_def_dim(ncid, "layers", layers_len, &layers_dim);
    check_err(stat,__LINE__,__FILE__);

    /* define variables */

    grid_center_lat_dims[0] = cells_dim;
    stat = nc_def_var(ncid, "grid_center_lat", NC_FLOAT, RANK_grid_center_lat, grid_center_lat_dims, &grid_center_lat_id);
    check_err(stat,__LINE__,__FILE__);

    grid_center_lon_dims[0] = cells_dim;
    stat = nc_def_var(ncid, "grid_center_lon", NC_FLOAT, RANK_grid_center_lon, grid_center_lon_dims, &grid_center_lon_id);
    check_err(stat,__LINE__,__FILE__);

    grid_edge_lat_dims[0] = edges_dim;
    stat = nc_def_var(ncid, "grid_edge_lat", NC_FLOAT, RANK_grid_edge_lat, grid_edge_lat_dims, &grid_edge_lat_id);
    check_err(stat,__LINE__,__FILE__);

    grid_edge_lon_dims[0] = edges_dim;
    stat = nc_def_var(ncid, "grid_edge_lon", NC_FLOAT, RANK_grid_edge_lon, grid_edge_lon_dims, &grid_edge_lon_id);
    check_err(stat,__LINE__,__FILE__);

    edge_var_dims[0] = time_dim;
    edge_var_dims[1] = edges_dim;
    edge_var_dims[2] = layers_dim;
    stat = nc_def_var(ncid, "edge_var", NC_FLOAT, RANK_edge_var, edge_var_dims, &edge_var_id);
    check_err(stat,__LINE__,__FILE__);

    /* leave define mode */
    stat = nc_enddef(ncid);
    check_err(stat,__LINE__,__FILE__);
    stat = nc_close(ncid);
    check_err(stat,__LINE__,__FILE__);
    return 0;
}
