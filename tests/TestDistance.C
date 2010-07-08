/**
 * An incredibly naive approach for helping determine a distance-weighted
 * average.
 *
 * A fundamental problem is finding nearest neighbors.  There are certainly
 * more efficient algorithms.  This is a brute force method.  Every cell
 * calculates its distance from a destination grid cell.
 *
 * This code exists to dis/prove whether this method is tractable.
 *
 * We assume the input file has grid_center_lat/lon variables which share a
 * grid_cells dimension.  An arbitrary destination grid point is selected.
 */
#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <mpi.h>
#include <ga.h>
#include <macdecls.h>
#include <pnetcdf.h>

#include <cmath>
#include <iostream>

#include "Debug.H"
#include "Pnetcdf.H"

int main(int argc, char **argv)
{
    char *name_lat = "grid_center_lat";
    char *name_lon = "grid_center_lon";
    int src_ncid = 0;
    int src_varid_lat = 0;
    int src_varid_lon = 0;
    int src_dimid_ctr = 0;
    int64_t src_shape = 0;
    int64_t src_lo = 0;
    int64_t src_hi = 0;
    MPI_Offset src_dimlen = 0;
    MPI_Offset src_start = 0;
    MPI_Offset src_count = 0;
    int src_g_lat = 0;
    int src_g_lon = 0;
    float *src_lat = NULL;
    float *src_lon = NULL;

    int dst_ncid = 0;
    int dst_varid_lat = 0;
    int dst_varid_lon = 0;
    int dst_dimid_ctr = 0;
    int64_t dst_shape = 0;
    int64_t dst_lo = 0;
    int64_t dst_hi = 0;
    MPI_Offset dst_dimlen = 0;
    MPI_Offset dst_start = 0;
    MPI_Offset dst_count = 0;
    int dst_g_lat = 0;
    int dst_g_lon = 0;
    float *dst_lat = NULL;
    float *dst_lon = NULL;
    float dst_latval;
    float dst_lonval;

    int g_distance = 0;
    float *distance = NULL;
    int64_t index = 0;
    float max = 0;
    float min = 0;

    MPI_Init(&argc, &argv);
    GA_Initialize();

    if (3 != argc) {
        pagoda::print_zero("Usage: TestDistance <source grid filename> <destination grid filename>\n");
        for (int i=0; i<argc; ++i) {
            pagoda::print_zero("argv[%d]=%s\n", i, argv[i]);
        }
        return 1;
    }

    ncmpi::open(MPI_COMM_WORLD, argv[1], NC_NOWRITE, MPI_INFO_NULL, &src_ncid);
    ncmpi::inq_varid(src_ncid, name_lat, &src_varid_lat);
    ncmpi::inq_varid(src_ncid, name_lon, &src_varid_lon);
    ncmpi::inq_vardimid(src_ncid, src_varid_lat, &src_dimid_ctr);
    ncmpi::inq_dimlen(src_ncid, src_dimid_ctr, &src_dimlen);
    src_shape = src_dimlen;
    src_g_lat = NGA_Create64(C_FLOAT, 1, &src_shape, "lat", NULL);
    src_g_lon = NGA_Create64(C_FLOAT, 1, &src_shape, "lon", NULL);
    g_distance = NGA_Create64(C_FLOAT, 1, &src_shape, "distance", NULL);
    NGA_Distribution64(src_g_lat, GA_Nodeid(), &src_lo, &src_hi);

    ncmpi::open(MPI_COMM_WORLD, argv[2], NC_NOWRITE, MPI_INFO_NULL, &dst_ncid);
    ncmpi::inq_varid(dst_ncid, name_lat, &dst_varid_lat);
    ncmpi::inq_varid(dst_ncid, name_lon, &dst_varid_lon);
    ncmpi::inq_vardimid(dst_ncid, dst_varid_lat, &dst_dimid_ctr);
    ncmpi::inq_dimlen(dst_ncid, dst_dimid_ctr, &dst_dimlen);
    dst_shape = dst_dimlen;
    dst_g_lat = NGA_Create64(C_FLOAT, 1, &dst_shape, "lat", NULL);
    dst_g_lon = NGA_Create64(C_FLOAT, 1, &dst_shape, "lon", NULL);
    NGA_Distribution64(dst_g_lat, GA_Nodeid(), &dst_lo, &dst_hi);

    // Read in src lat/lon.
    if (0 > src_lo && 0 > src_hi) {
        src_start = 0;
        src_count = 0;
        ncmpi::get_vara_all(src_ncid, src_varid_lat, &src_start, &src_count, src_lat);
        ncmpi::get_vara_all(src_ncid, src_varid_lon, &src_start, &src_count, src_lon);
    } else {
        src_start = src_lo;
        src_count = src_hi-src_lo+1;
        NGA_Access64(src_g_lat, &src_lo, &src_hi, &src_lat, NULL);
        NGA_Access64(src_g_lon, &src_lo, &src_hi, &src_lon, NULL);
        ncmpi::get_vara_all(src_ncid, src_varid_lat, &src_start, &src_count, src_lat);
        ncmpi::get_vara_all(src_ncid, src_varid_lon, &src_start, &src_count, src_lon);
        NGA_Release_update64(src_g_lat, &src_lo, &src_hi);
        NGA_Release_update64(src_g_lon, &src_lo, &src_hi);
    }

    // Read in dst lat/lon.
    if (0 > dst_lo && 0 > dst_hi) {
        dst_start = 0;
        dst_count = 0;
        ncmpi::get_vara_all(dst_ncid, dst_varid_lat, &dst_start, &dst_count, dst_lat);
        ncmpi::get_vara_all(dst_ncid, dst_varid_lon, &dst_start, &dst_count, dst_lon);
    } else {
        dst_start = dst_lo;
        dst_count = dst_hi-dst_lo+1;
        NGA_Access64(dst_g_lat, &dst_lo, &dst_hi, &dst_lat, NULL);
        NGA_Access64(dst_g_lon, &dst_lo, &dst_hi, &dst_lon, NULL);
        ncmpi::get_vara_all(dst_ncid, dst_varid_lat, &dst_start, &dst_count, dst_lat);
        ncmpi::get_vara_all(dst_ncid, dst_varid_lon, &dst_start, &dst_count, dst_lon);
        NGA_Release_update64(dst_g_lat, &dst_lo, &dst_hi);
        NGA_Release_update64(dst_g_lon, &dst_lo, &dst_hi);
    }

    // Calculate distances.  Iterate over dst.
    // Get dst lat/lon, then iterate over local src lat/lon.
    if (0 > src_lo && 0 > src_hi) {
        GA_Sync();
        NGA_Select_elem64(g_distance, "max", &max, &index);
        NGA_Select_elem64(g_distance, "min", &min, &index);
    } else {
        NGA_Access64(g_distance, &src_lo, &src_hi, &distance, NULL);
        for (int64_t dst_i=0; dst_i<dst_shape; ++dst_i) {
            NGA_Get64(dst_g_lat, &dst_i, &dst_i, &dst_latval, NULL);
            NGA_Get64(dst_g_lon, &dst_i, &dst_i, &dst_lonval, NULL);
            for (int i=0, n=src_hi-src_lo+1; i<n; ++i) {
                distance[i] = acos(cos(dst_latval)*cos(src_lat[i])*(cos(dst_lonval)*cos(src_lon[i]) + sin(dst_lonval)*sin(src_lon[i])) + sin(dst_latval)*sin(src_lat[i]));
            }
            GA_Sync();
            NGA_Select_elem64(g_distance, "max", &max, &index);
            //pagoda::print_zero("max=%f\tindex=%lld\n", max, index);
            NGA_Select_elem64(g_distance, "min", &min, &index);
            //pagoda::print_zero("min=%f\tindex=%lld\n", min, index);
            if (dst_i % 10 == 0) {
                pagoda::print_zero("%lld/%lld\n", dst_i, dst_shape);
            }
        }
        NGA_Release_update64(g_distance, &src_lo, &src_hi);
    }

    ncmpi::close(src_ncid);

    GA_Terminate();
    MPI_Finalize();

    return 0;
}
