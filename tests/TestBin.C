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

#include <cfloat>
#include <cmath>
#include <map>
#include <vector>
using std::fabs;
using std::map;
using std::vector;

#include <mpi.h>
#include <ga.h>
#include <macdecls.h>
#include <pnetcdf.h>

#include "Debug.H"
#include "PnetcdfTiming.H"

void check(int err) {
    if (MPI_SUCCESS != err) {
        GA_Error("MPI call failed", err);
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
    map<int,vector<float> > bin_lat;
    map<int,vector<float> > bin_lon;
    int g_bin = 0;
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

    g_bin = NGA_Create64(C_INT, 1, &nbins, "bin", NULL);
    NGA_Distribution64(g_bin, me, &bin_lo, &bin_hi);

    ncmpi::open(MPI_COMM_WORLD, argv[1], NC_NOWRITE, MPI_INFO_NULL, &src_ncid);
    ncmpi::inq_varid(src_ncid, name_lat, &src_varid_lat);
    ncmpi::inq_varid(src_ncid, name_lon, &src_varid_lon);
    ncmpi::inq_vardimid(src_ncid, src_varid_lat, &src_dimid_ctr);
    ncmpi::inq_dimlen(src_ncid, src_dimid_ctr, &src_dimlen);
    src_shape = src_dimlen;
    g_src_lat = NGA_Create64(C_FLOAT, 1, &src_shape, "lat", NULL);
    g_src_lon = NGA_Create64(C_FLOAT, 1, &src_shape, "lon", NULL);
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
        ncmpi::get_vara_all(src_ncid, src_varid_lat, &src_start, &src_count, src_lat);
        ncmpi::get_vara_all(src_ncid, src_varid_lon, &src_start, &src_count, src_lon);
    } else {
        src_start = src_lo;
        src_count = src_hi-src_lo+1;
        NGA_Access64(g_src_lat, &src_lo, &src_hi, &src_lat, NULL);
        NGA_Access64(g_src_lon, &src_lo, &src_hi, &src_lon, NULL);
        ncmpi::get_vara_all(src_ncid, src_varid_lat, &src_start, &src_count, src_lat);
        ncmpi::get_vara_all(src_ncid, src_varid_lon, &src_start, &src_count, src_lon);
        NGA_Release_update64(g_src_lat, &src_lo, &src_hi);
        NGA_Release_update64(g_src_lon, &src_lo, &src_hi);
    }

    NGA_Select_elem64(g_src_lat, "min", &lat_min, &lat_min_idx);
    NGA_Select_elem64(g_src_lat, "max", &lat_max, &lat_max_idx);

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

    // First, iterate over the data we own and bin it if it belongs here.
    if (0 > src_lo && 0 > src_hi) {
        // nothing to do
    } else {
        NGA_Access64(g_src_lat, &src_lo, &src_hi, &src_lat, NULL);
        NGA_Access64(g_src_lon, &src_lo, &src_hi, &src_lon, NULL);
        for (int64_t i=0,limit=src_hi-src_lo+1; i<limit; ++i) {
            if (src_lat[i] < (lat_min+bin_lo*portion)) {
                // src_lat[i] is less than the smallest bin
                skipped_lat.push_back(src_lat[i]);
                skipped_lon.push_back(src_lon[i]);
                continue;
            } else if (src_lat[i] >= (lat_min+(bin_hi+1)*portion)) {
                // src_lat[i] is greater than the largest bin
                skipped_lat.push_back(src_lat[i]);
                skipped_lon.push_back(src_lon[i]);
                continue;
            } else {
                // this is a linear search; could be improved with a binary one
                for (int64_t binidx=bin_lo; binidx<=bin_hi; ++binidx) {
                    if (src_lat[i] < (lat_min+(binidx+1)*portion)) {
                        bin_lat[binidx].push_back(src_lat[i]);
                        bin_lon[binidx].push_back(src_lon[i]);
                        break;
                    }
                }
            }
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

        check(MPI_Isend(&skipped_lat[0], skipped_lat.size(), MPI_FLOAT, send_to, 0, MPI_COMM_WORLD, &request));
        check(MPI_Recv(received_lat, buffsize, MPI_FLOAT, recv_from, 0, MPI_COMM_WORLD, &recv_status));
        check(MPI_Wait(&request, &wait_status));
        check(MPI_Get_count(&recv_status, MPI_FLOAT, &received_lat_count));
        PRINT_SYNC("lat sent=%lu,recv=%d\n",
                (long unsigned)skipped_lat.size(), received_lat_count);

        check(MPI_Isend(&skipped_lon[0], skipped_lon.size(), MPI_FLOAT, send_to, 0, MPI_COMM_WORLD, &request));
        check(MPI_Recv(received_lon, buffsize, MPI_FLOAT, recv_from, 0, MPI_COMM_WORLD, &recv_status));
        check(MPI_Wait(&request, &wait_status));
        check(MPI_Get_count(&recv_status, MPI_FLOAT, &received_lon_count));
        PRINT_SYNC("lon sent=%lu,recv=%d\n",
                (long unsigned)skipped_lon.size(), received_lon_count);

        // At this point we have sent/recv skipped data.
        // Iterate over what we received and rebuild the skipped vectors.
        skipped_lat.clear();
        skipped_lon.clear();
        for (int i=0; i<received_lat_count; ++i) {
            if (received_lat[i] < (lat_min+bin_lo*portion)) {
                // received_lat[i] is less than the smallest bin
                skipped_lat.push_back(received_lat[i]);
                skipped_lon.push_back(received_lon[i]);
                continue;
            } else if (received_lat[i] >= (lat_min+(bin_hi+1)*portion)) {
                // received_lat[i] is greater than the largest bin
                skipped_lat.push_back(received_lat[i]);
                skipped_lon.push_back(received_lon[i]);
                continue;
            } else {
                // this is a linear search; could be improved with a binary one
                for (int64_t binidx=bin_lo; binidx<=bin_hi; ++binidx) {
                    if (received_lat[i] < (lat_min+(binidx+1)*portion)) {
                        bin_lat[binidx].push_back(received_lat[i]);
                        bin_lon[binidx].push_back(received_lon[i]);
                        break;
                    }
                }
            }
        }
    }

    PRINT_SYNC("bin_lat.size()=%lu\n", (long unsigned)bin_lat.size());

    long unsigned points = 0;
    map<int,vector<float> >::iterator it,end;
    for (it=bin_lat.begin(),end=bin_lat.end(); it!=end; ++it) {
        points += it->second.size();
    }
    PRINT_SYNC("points=%lu\n", points);

    // Cleanup.
    ncmpi::close(src_ncid);

    GA_Terminate();
    MPI_Finalize();
}
