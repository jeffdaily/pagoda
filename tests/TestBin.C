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


static void check(int err);
static void read_data(int g_a, int ncid, int varid);
static int create_array_and_read(int ncid, char *name);
static int calc_extents_minmax(int g_lat);
static void do_bin(
        bin_t &bin_lat, bin_t &bin_lon, float *extents, int64_t nbins,
        float *lat, float *lon, int n,
        vector<float> &skipped_lat, vector<float> &skipped_lon);
static void bin(
        bin_t &bin_lat, bin_t &bin_lon,
        int g_lat, int g_lon, int g_extents);


int main(int argc, char **argv)
{
    int me = -1;
    int nproc = -1;
    char *name_lat = "grid_center_lat";
    char *name_lon = "grid_center_lon";

    int src_ncid = 0;
    int64_t src_lo = 0;
    int64_t src_hi = 0;
    int g_src_lat = 0;
    int g_src_lon = 0;

    int dst_ncid = 0;
    int64_t dst_lo = 0;
    int64_t dst_hi = 0;
    int g_dst_lat = 0;
    int g_dst_lon = 0;

    bin_t src_bin_lat;
    bin_t src_bin_lon;
    bin_t dst_bin_lat;
    bin_t dst_bin_lon;
    int g_bin_extents = 0;

    MPI_Init(&argc, &argv);
    GA_Initialize();

    me = GA_Nodeid();
    nproc = GA_Nnodes();

    if (3 != argc) {
        PRINT_ZERO("Usage: TestSort <source grid> <dest grid>\n");
        for (int i=0; i<argc; ++i) {
            PRINT_ZERO("argv[%d]=%s\n", i, argv[i]);
        }
        return 1;
    }

    // Open the source file.  Create g_a's and read lat/lons.
    ncmpi::open(MPI_COMM_WORLD, argv[1], NC_NOWRITE, MPI_INFO_NULL, &src_ncid);
    g_src_lat = create_array_and_read(src_ncid, name_lat);
    g_src_lon = create_array_and_read(src_ncid, name_lon);
    NGA_Distribution64(g_src_lat, me, &src_lo, &src_hi);

    // Open the destination file.  Create g_a's and read lat/lons.
    ncmpi::open(MPI_COMM_WORLD, argv[2], NC_NOWRITE, MPI_INFO_NULL, &dst_ncid);
    g_dst_lat = create_array_and_read(dst_ncid, name_lat);
    g_dst_lon = create_array_and_read(dst_ncid, name_lon);
    NGA_Distribution64(g_dst_lat, me, &dst_lo, &dst_hi);

    g_bin_extents = calc_extents_minmax(g_src_lat);

    bin(src_bin_lat, src_bin_lon, g_src_lat, g_src_lon, g_bin_extents);
    bin(dst_bin_lat, dst_bin_lon, g_dst_lat, g_dst_lon, g_bin_extents);

    PRINT_SYNC("src_bin_lat.size()=%lu\n", (long unsigned)src_bin_lat.size());

    long unsigned points = 0;
    bin_t::iterator it,end;
    for (it=src_bin_lat.begin(),end=src_bin_lat.end(); it!=end; ++it) {
        points += it->second.size();
    }
    PRINT_SYNC("points=%lu\n", points);

    // Cleanup.
    ncmpi::close(src_ncid);
    ncmpi::close(dst_ncid);

    GA_Terminate();
    MPI_Finalize();
}


static void check(int err)
{
    if (MPI_SUCCESS != err) {
        GA_Error("MPI call failed", err);
    }
}


static void read_data(int g_a, int ncid, int varid)
{
    int me = GA_Nodeid();
    int64_t lo;
    int64_t hi;
    MPI_Offset start;
    MPI_Offset count;
    float *data=NULL;

    NGA_Distribution64(g_a, me, &lo, &hi);

    if (0 > lo && 0 > hi) {
        start = 0;
        count = 0;
        ncmpi::get_vara_all(ncid, varid, &start, &count, data);
    } else {
        start = lo;
        count = hi-lo+1;
        NGA_Access64(g_a, &lo, &hi, &data, NULL);
        ncmpi::get_vara_all(ncid, varid, &start, &count, data);
        NGA_Release_update64(g_a, &lo, &hi);
    }
}


// Open the source file.  Create g_a's to hold lat/lons.
static int create_array_and_read(int ncid, char *name)
{
    int varid;
    int dimid;
    MPI_Offset dimlen;
    int64_t shape;
    int g_a;

    ncmpi::inq_varid(ncid, name, &varid);
    ncmpi::inq_vardimid(ncid, varid, &dimid);
    ncmpi::inq_dimlen(ncid, dimid, &dimlen);
    shape = dimlen;
    g_a = NGA_Create64(C_FLOAT, 1, &shape, name, NULL);
    read_data(g_a, ncid, varid);

    return g_a;
}


static int calc_extents_minmax(int g_lat)
{
    int me = GA_Nodeid();
    int64_t nbins = 1000; // arbitrary
    float lat_min;
    float lat_max;
    int64_t lat_min_idx;
    int64_t lat_max_idx;
    float diff;
    float portion;
    int g_extents;
    int64_t bin_lo;
    int64_t bin_hi;
    int64_t bin_extents_lo[2];
    int64_t bin_extents_hi[2];
    int64_t bin_extents_dims[2] = {nbins,2};
    int64_t bin_extents_chunk[2] = {0,2};
    float *bin_extents;
    int64_t bin_extents_ld;

    // Create bin extents g_a.
    // Procs might want to know the bin ranges owned by other procs.
    g_extents = NGA_Create64(C_FLOAT, 2, bin_extents_dims,
            "bin_extents", bin_extents_chunk);
    NGA_Distribution64(g_extents, me, bin_extents_lo, bin_extents_hi);
    bin_lo = bin_extents_lo[0];
    bin_hi = bin_extents_hi[0];
    PRINT_SYNC("bin_extents lo,hi = {%ld,%ld},{%ld,%ld}\n", bin_lo, bin_hi,
            bin_extents_hi[0], bin_extents_hi[1]);

    NGA_Select_elem64(g_lat, "min", &lat_min, &lat_min_idx);
    NGA_Select_elem64(g_lat, "max", &lat_max, &lat_max_idx);

    // Determine bin extents.  Currently the range for each bin is uniform.
    diff = fabs(lat_max-lat_min);
    portion = diff/nbins;
    PRINT_ZERO("    min=%f, idx=%ld\n", lat_min, lat_min_idx);
    PRINT_ZERO("    max=%f, idx=%ld\n", lat_max, lat_max_idx);
    PRINT_ZERO("   diff=%f\n", diff);
    PRINT_ZERO("portion=%f\n", portion);
    PRINT_SYNC("bin lo,hi=%ld,%ld\n", bin_lo, bin_hi);
    PRINT_SYNC("range=[%f,%f)\n",
            lat_min+bin_lo*portion, lat_min+(bin_hi+1)*portion);
    // Put bin extents into g_extents in case other procs need the info.
    if (0 > bin_lo && 0 > bin_hi) {
        // nothing to do
        PRINT_SYNC("bin_extents_ld=NONE\n");
    } else {
        NGA_Access64(g_extents, bin_extents_lo, bin_extents_hi,
                &bin_extents, &bin_extents_ld);
        PRINT_SYNC("bin_extents_ld={%ld}\n", bin_extents_ld);
        for (int64_t binidx=bin_lo;
                binidx<=bin_hi; ++binidx) {
            *bin_extents = binidx*portion + lat_min;
            ++bin_extents;
            *bin_extents = (binidx+1)*portion + lat_min;
            ++bin_extents;
        }
        NGA_Release_update64(g_extents, bin_extents_lo, bin_extents_hi);
    }

    return g_extents;
}


static void do_bin(
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


static void bin(
        bin_t &bin_lat, bin_t &bin_lon,
        int g_lat, int g_lon, int g_extents)
{
    int me = GA_Nodeid();
    int nproc = GA_Nnodes();
    int64_t lo;
    int64_t hi;
    int64_t limit;
    float *lat;
    float *lon;
    float *extents;
    int64_t buffsize;
    int64_t extents_lo[2];
    int64_t extents_hi[2];
    int64_t extents_ld;
    int64_t bin_lo;
    int64_t bin_hi;
    int64_t nbins;
    vector<float> skipped_lat;
    vector<float> skipped_lon;
    float *received_lat;
    float *received_lon;
    int received_lat_count;
    int received_lon_count;

    NGA_Distribution64(g_lat, me, &lo, &hi);
    NGA_Distribution64(g_extents, me, extents_lo, extents_hi);
    bin_lo = extents_lo[0];
    bin_hi = extents_hi[0];
    nbins = bin_hi-bin_lo+1;
    limit = hi-lo+1;
    buffsize = limit;
    GA_Lgop(&buffsize, 1, "max");
    PRINT_ZERO("buffsize=%ld\n", buffsize);
    skipped_lat.reserve(buffsize);
    skipped_lon.reserve(buffsize);
    received_lat = new float[buffsize];
    received_lon = new float[buffsize];

    // First, iterate over the data we own and bin it if it belongs here.
    if (0 > lo && 0 > hi) {
        // nothing to do
    } else {
        NGA_Access64(g_lat, &lo, &hi, &lat, NULL);
        NGA_Access64(g_lon, &lo, &hi, &lon, NULL);
        // It's possible to own data but not own any bins.
        if (0 > bin_lo && 0 > bin_hi) {
            // own data, but no bin extents
            // All data is "skipped".
            skipped_lat.assign(lat,lat+limit);
            skipped_lon.assign(lon,lon+limit);
        } else {
            NGA_Access64(g_extents, extents_lo, extents_hi,
                    &extents, &extents_ld);
            do_bin(bin_lat, bin_lon, extents, nbins,
                    lat, lon, limit, skipped_lat, skipped_lon);
            NGA_Release_update64(g_extents, extents_lo, extents_hi);
        }
        NGA_Release_update64(g_lat, &lo, &hi);
        NGA_Release_update64(g_lon, &lo, &hi);
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
            NGA_Access64(g_extents, extents_lo, extents_hi,
                    &extents, &extents_ld);
            do_bin(bin_lat, bin_lon, extents, nbins,
                    received_lat, received_lon, received_lat_count,
                    skipped_lat, skipped_lon);
            NGA_Release_update64(g_extents, extents_lo, extents_hi);
        }
    }
    delete [] received_lat;
    delete [] received_lon;
}

