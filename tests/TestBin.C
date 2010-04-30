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
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
using std::acos;
using std::cerr;
using std::cos;
using std::endl;
using std::fabs;
using std::lower_bound;
using std::map;
using std::max_element;
using std::min_element;
using std::ostringstream;
using std::pair;
using std::sin;
using std::sort;
using std::string;
using std::upper_bound;
using std::vector;

#include <mpi.h>
#include <ga.h>
#include <macdecls.h>
#include <pnetcdf.h>

#include "Debug.H"
#include "PnetcdfTiming.H"

#define EXTENTS_ARE_MONOTONICALLY_INCREASING 1
#define EPSILON 0.0001
#define N 3

class LatLonIdx
{
    public:
        float lat;
        float lon;
        int idx;

        LatLonIdx()
            : lat(0)
            , lon(0)
            , idx(0)
        {
        }
        LatLonIdx(float alat, float alon, int aidx)
            : lat(alat)
            , lon(alon)
            , idx(aidx)
        {
        }
        const string to_string()
        {
            ostringstream os("LatLonIdx(");
            os << lat << "," << lon << "," << idx << ")";
            return os.str();
        }
#if 0
        const bool operator< (const LatLonIdx &that)
        {
            if (this->lat == that.lat) {
                return this->lon < that.lon;
            }
            return this->lat < that.lat;
        }
#endif
};
const bool operator< (const LatLonIdx &ths, const LatLonIdx &that)
{
#if 0
    if (ths.lat == that.lat) {
        return ths.lon < that.lon;
    }
#endif
    return ths.lat < that.lat;
}

typedef pair<float,float> bin_index_t;
typedef map<bin_index_t,vector<LatLonIdx> > bin_t;
MPI_Datatype LatLonIdxType;

static void create_type();
static void check(int err);
static void read_data(int g_a, int ncid, int varid);
static int create_array_and_read(int ncid, char *name);
static int calc_extents_minmax(int g_lat);
static void bin_latitudes_worker(bin_t &bin, float *extents, int64_t nbins,
        float *lat, float *lon, int n, int offset, vector<LatLonIdx> &skipped);
static void bin_latitudes_worker(bin_t &bin, float *extents, int64_t nbins,
        LatLonIdx *data, int n, vector<LatLonIdx> &skipped);
static void bin_latitudes(bin_t &bin, int g_lat, int g_lon, int g_extents);
static void bin_sort(bin_t &bin);
static void calc_weights(int g_weights, int g_indices,
        const bin_t &src_bin, const bin_t &dst_bin, int n);
static float calc_distance(const LatLonIdx &src, const LatLonIdx &dst);


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

    bin_t src_bin;
    bin_t dst_bin;
    int g_bin_extents = 0;

    int type_ignore;
    int ndim_ignore;
    int64_t output_shape[2];
    int64_t output_chunk[2] = {0,N};
    int g_weights;
    int g_indices;
    float NEG_ONE_F = -1;
    int NEG_ONE_I = -1;

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

    create_type();

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

    bin_latitudes(src_bin, g_src_lat, g_src_lon, g_bin_extents);
    bin_latitudes(dst_bin, g_dst_lat, g_dst_lon, g_bin_extents);
    PRINT_SYNC("src_bin.size()=%lu\n", (long unsigned)src_bin.size());

    bin_sort(src_bin);
    bin_sort(dst_bin);
    PRINT_SYNC("finished sorting\n");

    // create output g_a's
    NGA_Inquire64(g_dst_lat, &type_ignore, &ndim_ignore, output_shape);
    output_shape[1] = N;
    output_chunk[0] = 0;
    output_chunk[1] = output_shape[1];
    PRINT_ZERO("output_shape={%ld,%ld} output_chunk={%ld,%ld}\n",
            output_shape[0], output_shape[1], output_chunk[0], output_chunk[1]);
    g_weights = NGA_Create64(C_FLOAT, 2, output_shape, "weights", output_chunk);
    g_indices = NGA_Create64(C_INT,   2, output_shape, "indices", output_chunk);
    GA_Fill(g_weights, &NEG_ONE_F);
    GA_Fill(g_indices, &NEG_ONE_I);
    calc_weights(g_weights, g_indices, src_bin, dst_bin, N);

    long unsigned points = 0;
    bin_t::iterator it,end;
    for (it=src_bin.begin(),end=src_bin.end(); it!=end; ++it) {
        points += it->second.size();
    }
    PRINT_SYNC("points=%lu\n", points);

    // Cleanup.
    ncmpi::close(src_ncid);
    ncmpi::close(dst_ncid);

    GA_Terminate();
    MPI_Finalize();
}


static void create_type()
{
    LatLonIdx instance[2];
    MPI_Datatype type[4] = {MPI_FLOAT, MPI_FLOAT, MPI_INT, MPI_UB};
    MPI_Aint start, disp[4];
    int blocklen[4] = {1, 1, 1, 1};

    MPI_Get_address(&instance[0], &start);
    MPI_Get_address(&instance[0].lat, &disp[0]);
    MPI_Get_address(&instance[0].lon, &disp[1]);
    MPI_Get_address(&instance[0].idx, &disp[2]);
    MPI_Get_address(&instance[1],     &disp[3]);
    for (int i=0; i<4; ++i) disp[i] -= start;
    MPI_Type_create_struct(4, blocklen, disp, type, &LatLonIdxType);
    MPI_Type_commit(&LatLonIdxType);
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
    PRINT_SYNC("bin_extents lo,hi = {%ld,%ld},{%ld,%ld}\n",
            bin_lo, bin_extents_lo[1], bin_hi, bin_extents_hi[1]);

    NGA_Select_elem64(g_lat, "min", &lat_min, &lat_min_idx);
    NGA_Select_elem64(g_lat, "max", &lat_max, &lat_max_idx);

    // Determine bin extents.  Currently the range for each bin is uniform.
    diff = fabs(lat_max-lat_min);
    portion = diff/nbins;
    PRINT_ZERO("    min=%f, idx=%ld\n", lat_min, lat_min_idx);
    PRINT_ZERO("    max=%f, idx=%ld\n", lat_max, lat_max_idx);
    PRINT_ZERO("   diff=%f\n", diff);
    PRINT_ZERO("portion=%f\n", portion);
    // Put bin extents into g_extents in case other procs need the info.
    if (0 > bin_lo && 0 > bin_hi) {
        // nothing to do
        PRINT_SYNC("range=NONE\n");
    } else {
        NGA_Access64(g_extents, bin_extents_lo, bin_extents_hi,
                &bin_extents, &bin_extents_ld);
        for (int64_t binidx=bin_lo;
                binidx<=bin_hi; ++binidx) {
            bin_extents[(binidx-bin_lo)*2]   =  binidx   *portion + lat_min;
            bin_extents[(binidx-bin_lo)*2+1] = (binidx+1)*portion + lat_min;
        }
        // fudge a little bit to make sure the first and last bins capture all
        // of the data
        if (0 == bin_lo) {
            bin_extents[0] -= EPSILON;
        }
        if ((nbins-1) == bin_hi) {
            bin_extents[(bin_hi-bin_lo)*2+1] += EPSILON;
        }
        PRINT_SYNC("range=[%f,%f)\n",
                bin_extents[0], bin_extents[(bin_hi-bin_lo)*2+1]);
        NGA_Release_update64(g_extents, bin_extents_lo, bin_extents_hi);
    }

    return g_extents;
}


static void bin_latitudes_worker(bin_t &bin, float *extents, int64_t nbins,
        float *lat, float *lon, int n, int offset, vector<LatLonIdx> &skipped)
{
#if EXTENTS_ARE_MONOTONICALLY_INCREASING
    // Optimization.
    // We know extents is monotonitcally increasing.  Immediately cull what
    // falls outside of min(extents) and max(extents).
    float ext_min = *min_element(extents,extents+(nbins*2));
    float ext_max = *max_element(extents,extents+(nbins*2));
#endif
    for (int i=0; i<n; ++i) {
#if EXTENTS_ARE_MONOTONICALLY_INCREASING
        if (lat[i] < ext_min || lat[i] > ext_max) {
            skipped.push_back(LatLonIdx(lat[i],lon[i],i+offset));
        } else {
#endif
            int64_t j;
            for (j=0; j<nbins; ++j) {
                float lat_min=extents[j*2];
                float lat_max=extents[j*2+1];
                if (lat_min <= lat[i] && lat[i] < lat_max) {
                    bin_index_t binidx(lat_min,lat_max);
                    bin[binidx].push_back(LatLonIdx(lat[i],lon[i],i+offset));
                    break;
                }
            }
            if (j >= nbins) {
                skipped.push_back(LatLonIdx(lat[i],lon[i],i+offset));
            }
#if EXTENTS_ARE_MONOTONICALLY_INCREASING
        }
#endif
    }
}


static void bin_latitudes_worker(bin_t &bin, float *extents, int64_t nbins,
        LatLonIdx *data, int n, vector<LatLonIdx> &skipped)
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
        if (data[i].lat < ext_min || data[i].lat > ext_max) {
            skipped.push_back(data[i]);
        } else {
#endif
            int64_t j;
            for (j=0; j<nbins; ++j) {
                float lat_min=extents[j*2];
                float lat_max=extents[j*2+1];
                if (lat_min <= data[i].lat && data[i].lat < lat_max) {
                    bin_index_t binidx(lat_min,lat_max);
                    bin[binidx].push_back(data[i]);
                    break;
                }
            }
            if (j >= nbins) {
                skipped.push_back(data[i]);
            }
#if EXTENTS_ARE_MONOTONICALLY_INCREASING
        }
#endif
    }
}


static void bin_latitudes(bin_t &bin, int g_lat, int g_lon, int g_extents)
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
    vector<LatLonIdx> skipped;
    LatLonIdx *received;
    int received_count;

    NGA_Distribution64(g_lat, me, &lo, &hi);
    NGA_Distribution64(g_extents, me, extents_lo, extents_hi);
    bin_lo = extents_lo[0];
    bin_hi = extents_hi[0];
    nbins = bin_hi-bin_lo+1;
    limit = hi-lo+1;
    buffsize = limit;
    GA_Lgop(&buffsize, 1, "max");
    PRINT_ZERO("buffsize=%ld\n", buffsize);
    skipped.reserve(buffsize);
    received = new LatLonIdx[buffsize];

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
            for (int i=0; i<limit; ++i) {
                skipped.push_back(LatLonIdx(lat[i],lon[i],i+lo));
            }
        } else {
            NGA_Access64(g_extents, extents_lo, extents_hi,
                    &extents, &extents_ld);
            bin_latitudes_worker(bin, extents, nbins,
                    lat, lon, limit, lo, skipped);
            NGA_Release_update64(g_extents, extents_lo, extents_hi);
        }
        NGA_Release_update64(g_lat, &lo, &hi);
        NGA_Release_update64(g_lon, &lo, &hi);
    }
    PRINT_SYNC("bin.size()=%lu\n", (long unsigned)bin.size());

    // Now start sending all skipped data to the next process.
    int send_to = me + 1;
    int recv_from = me - 1;
    if (send_to >= nproc) send_to = 0;
    if (recv_from < 0) recv_from = nproc-1;
    for (int proc=0; proc<nproc; ++proc) {
        MPI_Request request;
        MPI_Status recv_status;
        MPI_Status wait_status;

        check(MPI_Isend(&skipped[0], skipped.size(), LatLonIdxType,
                    send_to, 0, MPI_COMM_WORLD, &request));
        check(MPI_Recv(received, buffsize, LatLonIdxType,
                    recv_from, 0, MPI_COMM_WORLD, &recv_status));
        check(MPI_Wait(&request, &wait_status));
        check(MPI_Get_count(&recv_status, LatLonIdxType, &received_count));
        PRINT_SYNC("sent=%lu,recv=%d\n",
                (long unsigned)skipped.size(), received_count);

        // At this point we have sent/recv skipped data.
        // Iterate over what we received and rebuild the skipped vectors.
        skipped.clear();
        // It's possible to have data but not own any bins.
        if (0 > bin_lo && 0 > bin_hi) {
            // have data, but no bin extents
            // All data is "skipped".
            skipped.assign(received,received+received_count);
        } else {
            NGA_Access64(g_extents, extents_lo, extents_hi,
                    &extents, &extents_ld);
            bin_latitudes_worker(bin, extents, nbins,
                    received, received_count, skipped);
            NGA_Release_update64(g_extents, extents_lo, extents_hi);
        }
    }
    delete [] received;
}


static void bin_sort(bin_t &bin)
{
    for (bin_t::iterator it=bin.begin(),end=bin.end(); it!=end; ++it) {
        sort(it->second.begin(), it->second.end());
    }
}


/**
 * Calculate weights from src grid to dst grid via the supplied bins.
 *
 * Iterate over bins owned by this process and locate nearest n points to all
 * points in the dst bin from the source grid.
 *
 * Assumes bins are sorted.
 */
static void calc_weights(int g_weights, int g_indices,
        const bin_t &src_bin, const bin_t &dst_bin, int n)
{
    PRINT_SYNC("BEGIN calc_weights\n");
    int binidx=-1;
    bin_t::const_iterator dst_bin_it = dst_bin.begin();
    bin_t::const_iterator dst_bin_end = dst_bin.end();
    int uh_oh = 0;

    // iterate over dst bins
    for (/*empty*/; dst_bin_it!=dst_bin_end; ++dst_bin_it) {
        bin_t::const_iterator src_bin_it;
        vector<LatLonIdx>::const_iterator dst_data_it;
        vector<LatLonIdx>::const_iterator dst_data_end;

        ++binidx;
        dst_data_it  = dst_bin_it->second.begin();
        dst_data_end = dst_bin_it->second.end();

        // acquire corresponding src bin for the current dst bin
        src_bin_it = src_bin.find(dst_bin_it->first);
        if (src_bin_it == src_bin.end()) {
            // this process has a dst_bin without a corresponding src_bin
            ++uh_oh;
            continue;
        }
        
        // iterate over the current dst bin's data points
        for (/*empty*/; dst_data_it!=dst_data_end; ++dst_data_it) {
            map<float,LatLonIdx> distances;
            map<float,LatLonIdx>::const_iterator dist_it;
            map<float,LatLonIdx>::const_iterator dist_end;
            vector<LatLonIdx>::const_iterator src_data_it;
            vector<LatLonIdx>::const_iterator src_data_end;
            vector<LatLonIdx>::const_iterator low,up;
            float weights[n];
            int indices[n];
            int64_t lo[2];
            int64_t hi[2];
            int64_t ld[1];
            float denominator = 0;
            int i;

            src_data_it = src_bin_it->second.begin();
            src_data_end = src_bin_it->second.end();

            // we get lower and upper bounds for the current dst point
            // and then expand the lower and upper bound search by the next
            // lowest or highest bound
            low = lower_bound(src_data_it, src_data_end, *dst_data_it);
            if (low != src_data_it) {
                --low;
                low = lower_bound(src_data_it, src_data_end, *low);
            }
            up  = upper_bound(src_data_it, src_data_end, *dst_data_it);
            if (up != src_data_end) {
                ++up;
                up = lower_bound(src_data_it, src_data_end, *up);
            }

            // iterate over src data to calc distances to current dst point
            // but only iterate within the lower and upper bounds
            for (src_data_it=low; src_data_it!=up; ++src_data_it) {
                distances[calc_distance(*src_data_it,*dst_data_it)]=*src_data_it;
            }
            // thankfully the stl map is already sorted
            dist_it  = distances.begin();
            dist_end = distances.end();
            for (i=0; i<n && dist_it!=dist_end; ++i,++dist_it) {
                denominator += 1.0/(dist_it->first + EPSILON);
            }
            dist_it  = distances.begin();
            for (i=0; i<n && dist_it!=dist_end; ++i,++dist_it) {
                weights[i] = (1/(dist_it->first + EPSILON))/denominator;
                if (weights[i] > 1 || weights[i] < 0) {
                    weights[i] = 0;
                }
                indices[i] = dist_it->second.idx;
            }
            /*
            if (i<n) {
                cerr << "[" << GA_Nodeid() << "] i < n for "
                    << dst_data_it->idx << endl;
            }
            */
            // just in case number of weights < n
            for (/*empty*/; i<n; ++i){
                weights[i] = 0;
                indices[i] = -1;
            }
            lo[0] = dst_data_it->idx;
            lo[1] = 0;
            hi[0] = dst_data_it->idx;
            hi[1] = n-1;
            ld[0] = n;
            NGA_Put64(g_weights, lo, hi, weights, ld);
            NGA_Put64(g_indices, lo, hi, indices, ld);
        }
    }
    PRINT_SYNC("dst bins without src bins=%d\n", uh_oh);
    int64_t print_lo[2] = {0,0};
    int64_t print_hi[2] = {4,2};
    GA_Sync();
    NGA_Print_patch64(g_weights, print_lo, print_hi, 1);
    NGA_Print_patch64(g_indices, print_lo, print_hi, 1);
}

static float calc_distance(const LatLonIdx &src, const LatLonIdx &dst)
{
    return acos(cos(dst.lat)*cos(src.lat)*(cos(dst.lon)*cos(src.lon) + sin(dst.lon)*sin(src.lon)) + sin(dst.lat)*sin(src.lat));
}
