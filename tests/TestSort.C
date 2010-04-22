/**
 * Attempt to sort the latitudes of the input file.
 *
 * This type of operations is useful for determining the distance-weighted
 * average to interpolate between two grids.
 *
 * The TestDistance program represents an O(n^2) approach which is horribly
 * slow.  This should hopefully be much faster.
 *
 * We assume for this program that the input file has grid_center_lat/lon
 * variables which share a grid_cells dimension.
 *
 * To perform the sort we first locally bin the values, then redistribute the
 * bins.  There is one bin per processor so that the work is hopefully evenly
 * distributed.  The bins need not have the same size range of values.  In
 * fact, since we are binning the latitudes, we want the ranges of the
 * outermost bins to be larger since there are fewer values near the poles.
 *
 * TODO This was abandoned... Yes, sorting a 1D is possible in parallel.  But
 * after I thought about it some more, I couldn't think of what to do with the
 * sorted latitudes once I had them.
 * This code was later adapted into the TestBin program since binning seemed
 * like the only other viable option.
 */
#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <cfloat>
#include <cmath>
#include <map>
#include <vector>
using std::fabs;
using std::fill;
using std::map;
using std::vector;

#include <mpi.h>
#include <ga.h>
#include <macdecls.h>
#include <pnetcdf.h>

#include "Debug.H"
#include "PnetcdfTiming.H"

void MY_Select_elem64(int, bool, float*, int64_t*);

int main(int argc, char **argv)
{
    int me = -1;
    int nproc = -1;
    char *name_lat = "grid_center_lat";
    int src_ncid = 0;
    int src_varid_lat = 0;
    int src_dimid_ctr = 0;
    int64_t src_shape = 0;
    int64_t src_lo = 0;
    int64_t src_hi = 0;
    MPI_Offset src_dimlen = 0;
    MPI_Offset src_start = 0;
    MPI_Offset src_count = 0;
    int src_g_lat = 0;
    float *src_lat = NULL;
    float lat_min = 0;
    float lat_max = 0;
    int64_t lat_min_idx = 0;
    int64_t lat_max_idx = 0;
    float diff;
    float portion;
    long *bin_counts;
    map<int,vector<float> > bin;


    MPI_Init(&argc, &argv);
    GA_Initialize();

    me = GA_Nodeid();
    nproc = GA_Nnodes();
    bin_counts = new long[nproc];
    fill(bin_counts, bin_counts+nproc, 0);

    if (2 != argc) {
        PRINT_ZERO("Usage: TestSort <filename>\n");
        for (int i=0; i<argc; ++i) {
            PRINT_ZERO("argv[%d]=%s\n", i, argv[i]);
        }
        return 1;
    }

    ncmpi::open(MPI_COMM_WORLD, argv[1], NC_NOWRITE, MPI_INFO_NULL, &src_ncid);
    ncmpi::inq_varid(src_ncid, name_lat, &src_varid_lat);
    ncmpi::inq_vardimid(src_ncid, src_varid_lat, &src_dimid_ctr);
    ncmpi::inq_dimlen(src_ncid, src_dimid_ctr, &src_dimlen);
    src_shape = src_dimlen;
    src_g_lat = NGA_Create64(C_FLOAT, 1, &src_shape, "lat", NULL);
    NGA_Distribution64(src_g_lat, me, &src_lo, &src_hi);

    // Read in src lat.
    if (0 > src_lo && 0 > src_hi) {
        src_start = 0;
        src_count = 0;
        ncmpi::get_vara_all(src_ncid, src_varid_lat, &src_start, &src_count, src_lat);
    } else {
        src_start = src_lo;
        src_count = src_hi-src_lo+1;
        NGA_Access64(src_g_lat, &src_lo, &src_hi, &src_lat, NULL);
        ncmpi::get_vara_all(src_ncid, src_varid_lat, &src_start, &src_count, src_lat);
        NGA_Release_update64(src_g_lat, &src_lo, &src_hi);
    }

    NGA_Select_elem64(src_g_lat, "min", &lat_min, &lat_min_idx);
    //MY_Select_elem64(src_g_lat, true, &lat_min, &lat_min_idx);
    NGA_Select_elem64(src_g_lat, "max", &lat_max, &lat_max_idx);
    //MY_Select_elem64(src_g_lat, false, &lat_max, &lat_max_idx);

    diff = fabs(lat_max-lat_min);
    portion = diff/nproc;
    PRINT_ZERO("    min=%f, idx=%ld\n", lat_min, lat_min_idx);
    PRINT_ZERO("    max=%f, idx=%ld\n", lat_max, lat_max_idx);
    PRINT_ZERO("   diff=%f\n", diff);
    PRINT_ZERO("portion=%f\n", portion);
    PRINT_SYNC("range=[%f,%f)\n", lat_min+me*portion, lat_min+(me+1)*portion);

    // Iterate over local lats and bin locally.
    if (0 > src_lo && 0 > src_hi) {
        // No local data
    } else {
        NGA_Access64(src_g_lat, &src_lo, &src_hi, &src_lat, NULL);
        for (int64_t i=0,limit=src_hi-src_lo+1; i<limit; ++i) {
            for (int j=0; j<nproc; ++j) {
                if (src_lat[i] < lat_min+((j+1)*portion)) {
                    bin[j].push_back(src_lat[i]);
                    ++bin_counts[j];
                    break;
                }
            }
        }
        NGA_Release_update64(src_g_lat, &src_lo, &src_hi);
    }
    GA_Lgop(bin_counts, nproc, "+");
    if (0 == me) {
        printf("bin_counts={%d", bin_counts[0]);
        for (int i=1; i<nproc; ++i) {
            printf(",%d", bin_counts[i]);
        }
        printf("}\n");
    }
    PRINT_SYNC("bin.size()=%lu\n", (long unsigned)bin.size());
    PRINT_SYNC("bin1.size()=%lu\n", (long unsigned)(bin.begin()->second.size()));
    PRINT_SYNC("bin[0][0])=%f\n", bin.begin()->second.at(0));
    
    // Cleanup.
    ncmpi::close(src_ncid);
    delete [] bin_counts;

    GA_Terminate();
    MPI_Finalize();
}


void MY_Select_elem64(int g_a, bool min, float *result, int64_t *index)
{
    int me = GA_Nodeid();
    int64_t lo;
    int64_t hi;
    float val;
    int64_t idx = 0;
    float *ptr;


    NGA_Distribution64(g_a, me, &lo, &hi);
    if (0 > lo && 0 > hi) {
        if (min) {
            val = FLT_MAX;
        } else {
            val = FLT_MIN;
        }
    } else {
        NGA_Access64(g_a, &lo, &hi, &ptr, NULL);
        val = ptr[0];
        if (min) {
            for (int64_t i=1,limit=hi-lo+1; i<limit; ++i) {
                if (ptr[i] < val) {
                    val = ptr[i];
                    idx = i+lo;
                }
            }
        } else {
            for (int64_t i=1,limit=hi-lo+1; i<limit; ++i) {
                if (ptr[i] > val) {
                    val = ptr[i];
                    idx = i+lo;
                }
            }
        }
    }
    if (min) {
        GA_Fgop(&val, 1, "min");
    } else {
        GA_Fgop(&val, 1, "max");
    }

    *result = val;
    *index = idx;
}
