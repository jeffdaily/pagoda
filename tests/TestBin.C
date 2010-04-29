/**
 * Attempt to bin the latitudes of the source and destination grids.
 *
 * This type of operations is useful for determining the distance-weighted
 * average to interpolate between two grids.
 *
 * The TestDistance program represents an O(n^2) approach which is horribly
 * slow.  This should hopefully be much faster.
 *
 * We assume for this program that the files have grid_center_lat/lon
 * variables which share a grid_cells dimension.
 *
 * Each process owns a set of bins.  Rather than redistribute bins calculated
 * locally, we redistribute the data P times such that each process sees all
 * of the data, eventually.  Once the redistribution has occurred, no more
 * communication is necessary as each process has a set of bins to work on.
 */
#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <cfloat>
#include <cmath>
#include <map>
#include <utility>
#include <vector>
using std::fabs;
using std::map;
using std::pair;
using std::vector;

#include <mpi.h>
#include <ga.h>
#include <macdecls.h>
#include <pnetcdf.h>

#include "Debug.H"
#include "PnetcdfTiming.H"

#define EXTENTS_ARE_MONOTONICALLY_INCREASING 1

typedef pair<float,float> bin_index_t;
typedef map<bin_index_t,vector<float> > bin_t;

static inline void check(int err) {
    if (MPI_SUCCESS != err) {
        GA_Error("MPI call failed", err);
    }
}

static inline void do_bin(
        bin_t &bin_lat, bin_t &bin_lon, float *extents, int64_t nbins,
        float *lat, float *lon, int n,
        vector<float> &skipped_lat, vector<float> &skipped_lon)
{
#if EXTENTS_ARE_MONOTONICALLY_INCREASING
    // Optimization.
    // We know extents is monotonitcally increasing.  Immediately cull what
    // falls outside of min(extents) and max(extents).
    float ext_min = *std::min_element(extents,extents+(nbins*2));
    float ext_max = *std::max_element(extents,extents+(nbins*2));
#endif
    for (int i=0; i<n; ++i) {
#if EXTENTS_ARE_MONOTONICALLY_INCREASING
        if (lat[i] < ext_min || lat[i] > ext_max) {
            skipped_lat.push_back(lat[i]);
            skipped_lon.push_back(lon[i]);
        } else {
#endif
            int64_t j;
            for (j=0; j<nbins; ++j) {
                float lat_min=extents[j*2];
                float lat_max=extents[j*2+1];
                if (lat_min <= lat[i] && lat[i] < lat_max) {
                    bin_index_t binidx(lat_min,lat_max);
                    bin_lat[binidx].push_back(lat[i]);
                    bin_lon[binidx].push_back(lon[i]);
                    break;
                }
            }
            if (j >= nbins) {
                skipped_lat.push_back(lat[i]);
                skipped_lon.push_back(lon[i]);
            }
#if EXTENTS_ARE_MONOTONICALLY_INCREASING
        }
#endif
    }
}

int main(int argc, char **argv)
{
    int me = -1;
    int nproc = -1;
    int64_t nbins = 1000; // arbitrary
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
    int g_src_lat = 0;
    int g_src_lon = 0;
    float *src_lat = NULL;
    float *src_lon = NULL;
    float lat_min = 0;
    float lat_max = 0;
    int64_t lat_min_idx = 0;
    int64_t lat_max_idx = 0;
    float diff;
    float portion;
    bin_t bin_lat;
    bin_t bin_lon;
    int g_bin_extents = 0;
    int64_t bin_extents_lo[2];
    int64_t bin_extents_hi[2];
    int64_t bin_extents_dims[2] = {nbins,2};
    int64_t bin_extents_chunk[2] = {0,2};
    float *bin_extents;
    int64_t bin_extents_ld;
    int64_t bin_lo = 0;
    int64_t bin_hi = 0;
    vector<float> skipped_lat;
    vector<float> skipped_lon;
    float *received_lat;
    float *received_lon;
    int received_lat_count;
    int received_lon_count;
    long buffsize = 0;

    MPI_Init(&argc, &argv);
    GA_Initialize();

    me = GA_Nodeid();
    nproc = GA_Nnodes();

    if (2 != argc) {
        PRINT_ZERO("Usage: TestSort <filename>\n");
        for (int i=0; i<argc; ++i) {
            PRINT_ZERO("argv[%d]=%s\n", i, argv[i]);
        }
        return 1;
    }

    // Create bin extents g_a.
    // Procs might want to know the bin ranges owned by other procs.
    g_bin_extents = NGA_Create64(C_FLOAT, 2, bin_extents_dims,
            "bin_extents", bin_extents_chunk);
    NGA_Distribution64(g_bin_extents, me, bin_extents_lo, bin_extents_hi);
    bin_lo = bin_extents_lo[0];
    bin_hi = bin_extents_hi[0];
    PRINT_SYNC("bin_extents lo,hi = {%ld,%ld},{%ld,%ld}\n",
            bin_extents_lo[0], bin_extents_lo[1],
            bin_extents_hi[0], bin_extents_hi[1]);

    // Open the input file.  Create g_a's to hold lat/lons.
    ncmpi::open(MPI_COMM_WORLD, argv[1], NC_NOWRITE, MPI_INFO_NULL, &src_ncid);
    ncmpi::inq_varid(src_ncid, name_lat, &src_varid_lat);
    ncmpi::inq_varid(src_ncid, name_lon, &src_varid_lon);
    ncmpi::inq_vardimid(src_ncid, src_varid_lat, &src_dimid_ctr);
    ncmpi::inq_dimlen(src_ncid, src_dimid_ctr, &src_dimlen);
    src_shape = src_dimlen;
    g_src_lat = NGA_Create64(C_FLOAT, 1, &src_shape, "lat", NULL);
    g_src_lon = NGA_Create64(C_FLOAT, 1, &src_shape, "lon", NULL);
    // Determine size of largest buffer used to pass lat/lon data around.
    NGA_Distribution64(g_src_lat, me, &src_lo, &src_hi);
    buffsize = src_hi-src_lo+1;
    GA_Lgop(&buffsize, 1, "max");
    PRINT_ZERO("buffsize=%ld\n", buffsize);
    skipped_lat.reserve(buffsize);
    skipped_lon.reserve(buffsize);
    received_lat = new float[buffsize];
    received_lon = new float[buffsize];

    // Read in src lat,lon.
    if (0 > src_lo && 0 > src_hi) {
        src_start = 0;
        src_count = 0;
        ncmpi::get_vara_all(src_ncid, src_varid_lat,
                &src_start, &src_count, src_lat);
        ncmpi::get_vara_all(src_ncid, src_varid_lon,
                &src_start, &src_count, src_lon);
    } else {
        src_start = src_lo;
        src_count = src_hi-src_lo+1;
        NGA_Access64(g_src_lat, &src_lo, &src_hi, &src_lat, NULL);
        NGA_Access64(g_src_lon, &src_lo, &src_hi, &src_lon, NULL);
        ncmpi::get_vara_all(src_ncid, src_varid_lat,
                &src_start, &src_count, src_lat);
        ncmpi::get_vara_all(src_ncid, src_varid_lon,
                &src_start, &src_count, src_lon);
        NGA_Release_update64(g_src_lat, &src_lo, &src_hi);
        NGA_Release_update64(g_src_lon, &src_lo, &src_hi);
    }

    NGA_Select_elem64(g_src_lat, "min", &lat_min, &lat_min_idx);
    NGA_Select_elem64(g_src_lat, "max", &lat_max, &lat_max_idx);

    // Determine bin extents.  Currently the range for each bin is uniform.
    diff = fabs(lat_max-lat_min);
    portion = diff/nbins;
    PRINT_ZERO("    min=%f, idx=%ld\n", lat_min, lat_min_idx);
    PRINT_ZERO("    max=%f, idx=%ld\n", lat_max, lat_max_idx);
    PRINT_ZERO("   diff=%f\n", diff);
    PRINT_ZERO("portion=%f\n", portion);
    PRINT_SYNC("src lo,hi=%ld,%ld\n", src_lo, src_hi);
    PRINT_SYNC("bin lo,hi=%ld,%ld\n", bin_lo, bin_hi);
    PRINT_SYNC("range=[%f,%f)\n",
            lat_min+bin_lo*portion, lat_min+(bin_hi+1)*portion);
    // Put bin extents into g_bin_extents in case other procs need the info.
    if (0 > bin_lo && 0 > bin_hi) {
        // nothing to do
        PRINT_SYNC("bin_extents_ld=NONE\n");
    } else {
        NGA_Access64(g_bin_extents, bin_extents_lo, bin_extents_hi,
                &bin_extents, &bin_extents_ld);
        PRINT_SYNC("bin_extents_ld={%ld}\n", bin_extents_ld);
        for (int64_t binidx=bin_lo;
                binidx<=bin_hi; ++binidx) {
            *bin_extents = binidx*portion + lat_min;
            ++bin_extents;
            *bin_extents = (binidx+1)*portion + lat_min;
            ++bin_extents;
        }
        NGA_Release_update64(g_bin_extents, bin_extents_lo, bin_extents_hi);
    }

    // First, iterate over the data we own and bin it if it belongs here.
    if (0 > src_lo && 0 > src_hi) {
        // nothing to do
    } else {
        NGA_Access64(g_src_lat, &src_lo, &src_hi, &src_lat, NULL);
        NGA_Access64(g_src_lon, &src_lo, &src_hi, &src_lon, NULL);
        int64_t limit = src_hi-src_lo+1;
        // It's possible to own data but not own any bins.
        if (0 > bin_lo && 0 > bin_hi) {
            // own data, but no bin extents
            // All data is "skipped".
            skipped_lat.assign(src_lat,src_lat+limit);
            skipped_lon.assign(src_lon,src_lon+limit);
        } else {
            NGA_Access64(g_bin_extents, bin_extents_lo, bin_extents_hi,
                    &bin_extents, &bin_extents_ld);
            int64_t nbins = bin_hi-bin_lo+1;
            do_bin(bin_lat, bin_lon, bin_extents, nbins,
                    src_lat, src_lon, limit, skipped_lat, skipped_lon);
            NGA_Release_update64(g_bin_extents, bin_extents_lo, bin_extents_hi);
        }
        NGA_Release_update64(g_src_lat, &src_lo, &src_hi);
        NGA_Release_update64(g_src_lon, &src_lo, &src_hi);
    }
    PRINT_SYNC("bin_lat.size()=%lu\n", (long unsigned)bin_lat.size());

    // Now start sending all skipped data to the next process.
    int send_to = me + 1;
    int recv_from = me - 1;
    if (send_to >= nproc) send_to = 0;
    if (recv_from < 0) recv_from = nproc-1;
    for (int proc=0; proc<nproc; ++proc) {
        MPI_Request request;
        MPI_Status recv_status;
        MPI_Status wait_status;

        check(MPI_Isend(&skipped_lat[0], skipped_lat.size(), MPI_FLOAT,
                    send_to, 0, MPI_COMM_WORLD, &request));
        check(MPI_Recv(received_lat, buffsize, MPI_FLOAT,
                    recv_from, 0, MPI_COMM_WORLD, &recv_status));
        check(MPI_Wait(&request, &wait_status));
        check(MPI_Get_count(&recv_status, MPI_FLOAT, &received_lat_count));
        PRINT_SYNC("lat sent=%lu,recv=%d\n",
                (long unsigned)skipped_lat.size(), received_lat_count);

        check(MPI_Isend(&skipped_lon[0], skipped_lon.size(), MPI_FLOAT,
                    send_to, 0, MPI_COMM_WORLD, &request));
        check(MPI_Recv(received_lon, buffsize, MPI_FLOAT,
                    recv_from, 0, MPI_COMM_WORLD, &recv_status));
        check(MPI_Wait(&request, &wait_status));
        check(MPI_Get_count(&recv_status, MPI_FLOAT, &received_lon_count));
        PRINT_SYNC("lon sent=%lu,recv=%d\n",
                (long unsigned)skipped_lon.size(), received_lon_count);

        // At this point we have sent/recv skipped data.
        // Iterate over what we received and rebuild the skipped vectors.
        skipped_lat.clear();
        skipped_lon.clear();
        // It's possible to have data but not own any bins.
        if (0 > bin_lo && 0 > bin_hi) {
            // have data, but no bin extents
            // All data is "skipped".
            skipped_lat.assign(received_lat,received_lat+received_lat_count);
            skipped_lon.assign(received_lon,received_lon+received_lon_count);
        } else {
            NGA_Access64(g_bin_extents, bin_extents_lo, bin_extents_hi,
                    &bin_extents, &bin_extents_ld);
            int64_t nbins = bin_hi-bin_lo+1;
            do_bin(bin_lat, bin_lon, bin_extents, nbins,
                    received_lat, received_lon, received_lat_count,
                    skipped_lat, skipped_lon);
            NGA_Release_update64(g_bin_extents, bin_extents_lo, bin_extents_hi);
        }
    }

    PRINT_SYNC("bin_lat.size()=%lu\n", (long unsigned)bin_lat.size());

    long unsigned points = 0;
    bin_t::iterator it,end;
    for (it=bin_lat.begin(),end=bin_lat.end(); it!=end; ++it) {
        points += it->second.size();
    }
    PRINT_SYNC("points=%lu\n", points);

    // Cleanup.
    ncmpi::close(src_ncid);

    GA_Terminate();
    MPI_Finalize();
}
