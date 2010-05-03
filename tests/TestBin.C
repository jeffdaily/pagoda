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
 *
 * NOTE: Bins are assumed to be in monotonically increasing order across
 * processes starting on process zero.  There was a brief time when any
 * process could have any bin, but this made optimizations difficult.
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
using std::max;
using std::max_element;
using std::min_element;
using std::ostringstream;
using std::pair;
using std::sin;
using std::sort;
using std::string;
using std::upper_bound;
using std::vector;

#if HAVE_UNISTD_H
#   include <unistd.h> // for getopt
#endif

#include <mpi.h>
#include <ga.h>
#include <macdecls.h>
#include <pnetcdf.h>

#include "Debug.H"
#include "PnetcdfTiming.H"

#define EPSILON 0.0001
#define N 3
#define TAG_GHOST 44678

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
};
const bool operator< (const LatLonIdx &ths, const LatLonIdx &that)
{
    return ths.lat < that.lat;
}
static long max(const long &a, const size_t&b) {
    return (b<a)?a:(long)b;
}

typedef pair<float,float> bin_index_t;
typedef map<bin_index_t,vector<LatLonIdx> > bin_t;
MPI_Datatype LatLonIdxType;

static void create_type();
static void check(int err);
static void read_data(int g_a, int ncid, int varid);
static int create_array_and_read(int ncid, char *name);
static int calc_extents_minmax(int g_lat);
static void bin_latitudes_create(bin_t &bin, float *extents, int64_t nbins);
static void bin_latitudes_worker(bin_t &bin, float *extents, int64_t nbins,
        float *lat, float *lon, int n, int offset, vector<LatLonIdx> &skipped);
static void bin_latitudes_worker(bin_t &bin, float *extents, int64_t nbins,
        LatLonIdx *data, int n, vector<LatLonIdx> &skipped);
static void bin_latitudes(bin_t &bin, int g_lat, int g_lon, int g_extents);
static void bin_sort(bin_t &bin);
static void bin_ghost_exchange(bin_t &bin, int g_extents);
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

    string usage=
"Usage: TestBin [options] source dest variable\n"
"Allowed options:\n"
"\n"
"  -s (source grid)           use if source file requires external grid\n"
"  -d (dest grid)             use if source file requires external grid\n";
    string filename_source_grid;
    string filename_source;
    string filename_dest_grid;
    string filename_dest;
    string variable_name;
    bool perform_ghost_exchange = false;
    int c;

    MPI_Init(&argc, &argv);
    GA_Initialize();

    me = GA_Nodeid();
    nproc = GA_Nnodes();

    opterr = 0;
    while ((c = getopt(argc,argv, "s:d:g")) != -1) {
        switch (c) {
            case 's':
                filename_source_grid = optarg;
                break;
            case 'd':
                filename_dest_grid = optarg;
                break;
            case 'g':
                perform_ghost_exchange = true;
                break;
            default:
                char e[2] = {optopt, '\0'};
                PRINT_ZERO("Unrecognized option: %s\n", e);
                PRINT_ZERO(usage);
                return 1;
        }
    }

    if (optind == argc) {
        PRINT_ZERO("ERROR: source data required\n");
        PRINT_ZERO(usage);
        return 1;
    } else if (optind+1 == argc) {
        PRINT_ZERO("ERROR: dest data required\n");
        PRINT_ZERO(usage);
        return 1;
    } else if (optind+2 == argc) {
        PRINT_ZERO("ERROR: variable name required\n");
        PRINT_ZERO(usage);
        return 1;
    } else if (optind+3 == argc) {
        filename_source = argv[optind];
        filename_dest = argv[optind+1];
        variable_name = argv[optind+2];
    } else {
        PRINT_ZERO("ERROR: too many positional arguments\n");
        PRINT_ZERO(usage);
        return 1;
    }

    if (filename_source_grid.empty()) {
        filename_source_grid = filename_source;
    }
    if (filename_dest_grid.empty()) {
        filename_dest_grid = filename_dest;
    }
    create_type();

    // Open the source file.  Create g_a's and read lat/lons.
    ncmpi::open(MPI_COMM_WORLD, filename_source_grid.c_str(), NC_NOWRITE,
            MPI_INFO_NULL, &src_ncid);
    g_src_lat = create_array_and_read(src_ncid, name_lat);
    g_src_lon = create_array_and_read(src_ncid, name_lon);
    NGA_Distribution64(g_src_lat, me, &src_lo, &src_hi);

    // Open the destination file.  Create g_a's and read lat/lons.
    ncmpi::open(MPI_COMM_WORLD, filename_dest_grid.c_str(), NC_NOWRITE,
            MPI_INFO_NULL, &dst_ncid);
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

    long unsigned points = 0;
    bin_t::iterator it,end;
    for (it=src_bin.begin(),end=src_bin.end(); it!=end; ++it) {
        points += it->second.size();
    }
    PRINT_SYNC("points=%lu\n", points);

    if (perform_ghost_exchange) {
        long unsigned points_after = 0;
        bin_ghost_exchange(src_bin, g_bin_extents);
        PRINT_SYNC("finished bin ghost exchange\n");
        for (it=src_bin.begin(),end=src_bin.end(); it!=end; ++it) {
            points_after += it->second.size();
        }
        PRINT_SYNC("points_after=%lu\tdiff=%lu\n",
                points_after, points_after-points);
    }

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

    // verify the weights (and indices? TODO) we computed


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


static void bin_latitudes_create(bin_t &bin, float *extents, int64_t nbins)
{
    for (int64_t i=0; i<nbins; ++i) {
        float lat_min = extents[i*2];
        float lat_max = extents[i*2+1];
        bin[bin_index_t(lat_min,lat_max)].clear();
    }
}


static void bin_latitudes_worker(bin_t &bin, float *extents, int64_t nbins,
        float *lat, float *lon, int n, int offset, vector<LatLonIdx> &skipped)
{
    // Optimization.
    // We know extents is monotonitcally increasing.  Immediately cull what
    // falls outside of min(extents) and max(extents).
    float ext_min = *min_element(extents,extents+(nbins*2));
    float ext_max = *max_element(extents,extents+(nbins*2));
    for (int i=0; i<n; ++i) {
        if (lat[i] < ext_min || lat[i] > ext_max) {
            skipped.push_back(LatLonIdx(lat[i],lon[i],i+offset));
        } else {
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
        }
    }
}


static void bin_latitudes_worker(bin_t &bin, float *extents, int64_t nbins,
        LatLonIdx *data, int n, vector<LatLonIdx> &skipped)
{
    // Optimization.
    // We know extents is monotonitcally increasing.  Immediately cull what
    // falls outside of min(extents) and max(extents).
    float ext_min = *std::min_element(extents,extents+(nbins*2));
    float ext_max = *std::max_element(extents,extents+(nbins*2));
    for (int i=0; i<n; ++i) {
        if (data[i].lat < ext_min || data[i].lat > ext_max) {
            skipped.push_back(data[i]);
        } else {
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
        }
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
    long buffsize;
    int64_t extents_lo[2];
    int64_t extents_hi[2];
    int64_t extents_ld;
    int64_t bin_lo;
    int64_t bin_hi;
    int64_t nbins;
    vector<LatLonIdx> skipped;
    LatLonIdx *received;
    int received_count;
    int send_to;
    int recv_from;

    NGA_Distribution64(g_lat, me, &lo, &hi);
    NGA_Distribution64(g_extents, me, extents_lo, extents_hi);
    bin_lo = extents_lo[0];
    bin_hi = extents_hi[0];
    nbins = bin_hi-bin_lo+1;
    limit = hi-lo+1;
    buffsize = limit;
    PRINT_SYNC("before lgop buffsize=%ld\n", buffsize);
    GA_Lgop(&buffsize, 1, "max");
    PRINT_SYNC(" after lgop buffsize=%ld\n", buffsize);
    skipped.reserve(buffsize);
    received = new LatLonIdx[buffsize];

    // Create/initialize the bins if this proc owns any.
    if (0 > bin_lo && 0 > bin_hi) {
        // nothing to do
    } else {
        NGA_Access64(g_extents, extents_lo, extents_hi, &extents, &extents_ld);
        bin_latitudes_create(bin, extents, nbins);
        NGA_Release_update64(g_extents, extents_lo, extents_hi);
    }

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
    send_to = me + 1;
    recv_from = me - 1;
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
 * Exchange data along latitude bin divisions.
 *
 * All processes "own" one or more bins, and each bin contains a fixed range
 * from [low,high).  This partitioning is imperfect since data near the
 * edges of the bins may not have enough data to correctly calculate the
 * weights.  Since the bins are sorted, exchange the values on the fringes of
 * the bins.  This involves both communication and exchanging among bins
 * within a process.
 */
static void bin_ghost_exchange(bin_t &bin, int g_extents)
{
    int me;
    int nproc;
    bin_t extras_lower;
    bin_t extras_upper;
    int64_t extents_lo[2];
    int64_t extents_hi[2];
    int64_t extents_ld[1];
    int64_t extents_dims[2];
    int extents_ndim;
    int extents_type;
    float *extents;
    int64_t firstproc_subscript[2] = {0,0};
    int64_t  lastproc_subscript[2] = {0,0};
    int firstproc;
    int lastproc;
    bin_t::iterator it;
    bin_t::iterator end;
    bin_t::iterator it_next;
    bin_t::iterator it_prev;
    LatLonIdx *lower_received;
    LatLonIdx *upper_received;
    int lower_received_count;
    int upper_received_count;
    long buffsize = 0;
    int send_to;
    int recv_from;
    MPI_Request request;
    MPI_Status recv_status;
    MPI_Status wait_status;

    me = GA_Nodeid();
    nproc = GA_Nnodes();

    NGA_Distribution64(g_extents, me, extents_lo, extents_hi);
    NGA_Inquire64(g_extents, &extents_type, &extents_ndim, extents_dims);
    lastproc_subscript[0] = extents_dims[0]-1;
    lastproc  = NGA_Locate64(g_extents, lastproc_subscript);
    firstproc = NGA_Locate64(g_extents, firstproc_subscript);
    PRINT_ZERO("firstproc,lastproc = %d,%d\n", firstproc, lastproc);
    
    if (0 > extents_lo[0] && 0 > extents_hi[0]) {
        // process doesn't own any bins
        PRINT_ZERO("blank\n");
        PRINT_ZERO("blank\n");
        PRINT_SYNC("before lgop buffsize=%ld\n", buffsize);
        GA_Lgop(&buffsize, 1, "max");
        PRINT_SYNC("after lgop buffsize=%ld\n", buffsize);
        PRINT_ZERO("blank\n");
        PRINT_ZERO("blank\n");
        PRINT_ZERO("blank\n");
        PRINT_ZERO("blank\n");
        PRINT_ZERO("blank\n");
        return;
    }

    // Collect the extras.
    PRINT_ZERO("before collect extras\n");
    for (it=bin.begin(),end=bin.end(); it!=end; ++it) {
        vector<LatLonIdx>::iterator low,up;
        if (!it->second.empty()) {
            low = lower_bound(it->second.begin(), it->second.end(),
                    it->second.back());
            extras_upper[it->first].assign(low,it->second.end());
            buffsize = max(buffsize,extras_upper[it->first].size());
        }
        if (!it->second.empty()) {
            up = upper_bound(it->second.begin(), it->second.end(),
                    it->second.front());
            extras_lower[it->first].assign(it->second.begin(),up);
            buffsize = max(buffsize,extras_lower[it->first].size());
        }
    }
    PRINT_ZERO("after collect extras\n");
    PRINT_SYNC("before lgop buffsize=%ld\n", buffsize);
    GA_Lgop(&buffsize, 1, "max");
    PRINT_SYNC("after lgop buffsize=%ld\n", buffsize);
    lower_received = new LatLonIdx[buffsize];
    upper_received = new LatLonIdx[buffsize];

    PRINT_ZERO("before sending left\n");
    // First, ship the lowest left.
    send_to = me - 1;
    recv_from = me + 1;
    // All but the first proc sends left.
    if (me != firstproc) {
        check(MPI_Isend(&extras_lower.begin()->second[0],
                extras_lower.begin()->second.size(), LatLonIdxType,
                send_to, 0, MPI_COMM_WORLD, &request));
    }
    // All but the last proc receives.
    if (me != lastproc) {
        check(MPI_Recv(lower_received, buffsize, LatLonIdxType,
                recv_from, 0, MPI_COMM_WORLD, &recv_status));
        check(MPI_Get_count(&recv_status,LatLonIdxType,&lower_received_count));
    }
    // All but the first proc waits on the send.
    if (me != firstproc) {
        check(MPI_Wait(&request, &wait_status));
    }
    PRINT_ZERO("after sending left\n");

    // Next, ship the highest right.
    send_to = me + 1;
    recv_from = me - 1;
    // All but the last proc sends right.
    if (me != lastproc) {
        check(MPI_Isend(&extras_upper.begin()->second[0],
                extras_upper.begin()->second.size(), LatLonIdxType,
                send_to, 0, MPI_COMM_WORLD, &request));
    }
    // All but the first proc receives.
    if (me != firstproc) {
        check(MPI_Recv(upper_received, buffsize, LatLonIdxType,
                recv_from, 0, MPI_COMM_WORLD, &recv_status));
        check(MPI_Get_count(&recv_status,LatLonIdxType,&upper_received_count));
    }
    // All but the last proc waits on the send.
    if (me != lastproc) {
        check(MPI_Wait(&request, &wait_status));
    }
    PRINT_ZERO("after sending right\n");

    PRINT_ZERO("before insertions\n");
    // First, append received from right into last bin.
    if (me != lastproc) {
        it = --bin.end();
        it->second.insert(it->second.end(),
                lower_received, lower_received+lower_received_count);
    }
    // Second, insert received from left into first bin.
    if (me != firstproc) {
        it = bin.begin();
        it->second.insert(it->second.begin(),
                upper_received, upper_received+upper_received_count);
    }
    // Third, append local next-to-first.
    it = bin.begin();
    it_next = ++extras_lower.begin();
    if (it_next != extras_lower.end()) {
        it->second.insert(it->second.end(),
                it_next->second.begin(), it_next->second.end());
    }
    // Fourth, insert local next-to-last.
    it = --bin.end();
    it_prev = --extras_upper.end();
    if (it_prev != extras_upper.begin()) {
        --it_prev;
        it->second.insert(it->second.begin(),
                it_prev->second.begin(), it_prev->second.end());
    }
    // Lastly, insert within all middle bins.
    end = --bin.end();
    it_prev = extras_upper.begin();
    it = ++bin.begin();
    it_next = ++extras_lower.begin(); ++it_next;
    while (it != end) {
        it->second.insert(it->second.begin(),
                it_prev->second.begin(), it_prev->second.end());
        it->second.insert(it->second.end(),
                it_next->second.begin(), it_next->second.end());
        ++it;
        ++it_next;
        ++it_prev;
    }
    PRINT_ZERO("after insertions\n");

    delete [] lower_received;
    delete [] upper_received;
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
