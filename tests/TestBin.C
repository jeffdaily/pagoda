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
    return ((long)b<a)?a:(long)b;
}

typedef pair<float,float> bin_index_t;
typedef map<bin_index_t,vector<LatLonIdx> > bin_t;
MPI_Datatype LatLonIdxType;

static void create_type();
static void check(int err);
static void read_data(int g_a, int ncid, int varid, bool first_only=false);
static int create_array_and_read(int ncid, char *name, bool first_only=false);
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
static int interpolate(int g_data, int g_weights, int g_indices);
static void write_results(int g_results, int ncid_orig,
        const string& variablename, const string& filename);


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

    int data_ncid = 0;
    int g_data = 0;
    int g_results = 0;

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

    string usage= "Usage: TestBin sourcegrid destgrid datafile variable\n";
    string filename_grid_source;
    string filename_grid_destination;
    string filename_data;
    string variable_name;
    string outfile_name;
    bool perform_ghost_exchange = false;
    int c;

    MPI_Init(&argc, &argv);
    GA_Initialize();

    me = GA_Nodeid();
    nproc = GA_Nnodes();

    opterr = 0;
    while ((c = getopt(argc,argv, "go:")) != -1) {
        switch (c) {
            case 'g':
                perform_ghost_exchange = true;
                break;
            case 'o':
                outfile_name = optarg;
                break;
            default:
                char e[2] = {optopt, '\0'};
                PRINT_ZERO("Unrecognized option: %s\n", e);
                PRINT_ZERO(usage);
                return 1;
        }
    }

    if (optind == argc) {
        PRINT_ZERO("ERROR: source grid required\n");
        PRINT_ZERO(usage);
        return 1;
    } else if (optind+1 == argc) {
        PRINT_ZERO("ERROR: destination grid required\n");
        PRINT_ZERO(usage);
        return 1;
    } else if (optind+2 == argc) {
        PRINT_ZERO("ERROR: data file required\n");
        PRINT_ZERO(usage);
        return 1;
    } else if (optind+3 == argc) {
        PRINT_ZERO("ERROR: variable name required\n");
        PRINT_ZERO(usage);
        return 1;
    } else if (optind+4 == argc) {
        filename_grid_source = argv[optind];
        filename_grid_destination = argv[optind+1];
        filename_data = argv[optind+2];
        variable_name = argv[optind+3];
    } else {
        PRINT_ZERO("ERROR: too many positional arguments\n");
        PRINT_ZERO(usage);
        return 1;
    }
    if (outfile_name.empty()) {
        outfile_name = variable_name + "_out.nc";
    }

    create_type();

    // Open the source grid file.  Create g_a's and read lat/lons.
    ncmpi::open(MPI_COMM_WORLD, filename_grid_source.c_str(), NC_NOWRITE,
            MPI_INFO_NULL, &src_ncid);
    g_src_lat = create_array_and_read(src_ncid, name_lat);
    g_src_lon = create_array_and_read(src_ncid, name_lon);
    NGA_Distribution64(g_src_lat, me, &src_lo, &src_hi);

    // Open the destination grid file.  Create g_a's and read lat/lons.
    ncmpi::open(MPI_COMM_WORLD, filename_grid_destination.c_str(), NC_NOWRITE,
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
    PRINT_ZERO("finished sorting\n");

    long unsigned points = 0;
    bin_t::iterator it,end;
    for (it=src_bin.begin(),end=src_bin.end(); it!=end; ++it) {
        points += it->second.size();
    }
    PRINT_SYNC("points=%lu\n", points);

    if (perform_ghost_exchange) {
        long unsigned points_after = 0;
        bin_ghost_exchange(src_bin, g_bin_extents);
        PRINT_ZERO("finished bin ghost exchange\n");
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

    // verify the weights and indices we computed
    ncmpi::open(MPI_COMM_WORLD, filename_data.c_str(), NC_NOWRITE,
            MPI_INFO_NULL, &data_ncid);
    g_data = create_array_and_read(data_ncid,
            (char*)variable_name.c_str(), true);

    g_results = interpolate(g_data, g_weights, g_indices);

    // write the results to a file!
    PRINT_ZERO("before write_results\n");
    write_results(g_results, data_ncid, variable_name, outfile_name);

    PRINT_ZERO("before cleanup\n");
    // Cleanup.
    ncmpi::close(src_ncid);
    ncmpi::close(dst_ncid);
    ncmpi::close(data_ncid);

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


static void read_data(int g_a, int ncid, int varid, bool first_only)
{
    int me = GA_Nodeid();
    int ndims;
    int64_t lo[NC_MAX_VAR_DIMS];
    int64_t hi[NC_MAX_VAR_DIMS];
    int64_t ld[NC_MAX_VAR_DIMS];
    MPI_Offset start[NC_MAX_VAR_DIMS];
    MPI_Offset count[NC_MAX_VAR_DIMS];
    float *data=NULL;

    ncmpi::inq_varndims(ncid, varid, &ndims);
    NGA_Distribution64(g_a, me, lo, hi);

    if (0 > lo && 0 > hi) {
        for (int i=0; i<ndims; ++i) {
            start[i] = 0;
            count[i] = 0;
        }
        ncmpi::get_vara_all(ncid, varid, start, count, data);
    } else {
        if (first_only) {
            start[0] = 0;
            count[0] = 1;
            for (int i=1; i<ndims; ++i) {
                start[i] = lo[i-1];
                count[i] = hi[i-1]-lo[i-1]+1;
            }
        } else {
            for (int i=0; i<ndims; ++i) {
                start[i] = lo[i];
                count[i] = hi[i]-lo[i]+1;
            }
        }
        NGA_Access64(g_a, lo, hi, &data, ld);
        ncmpi::get_vara_all(ncid, varid, start, count, data);
        NGA_Release_update64(g_a, lo, hi);
    }
}


// Read the source file.  Create g_a's to hold variable.
static int create_array_and_read(int ncid, char *name, bool first_only)
{
    PRINT_ZERO("create_array_and_read first_only=%d name=%s\n", first_only, name);
    int varid;
    int ndim;
    int dimids[NC_MAX_VAR_DIMS];
    int64_t shape[NC_MAX_VAR_DIMS];
    int g_a;

    ncmpi::inq_varid(ncid, name, &varid);
    ncmpi::inq_varndims(ncid, varid, &ndim);
    ncmpi::inq_vardimid(ncid, varid, dimids);
    for (int i=0; i<ndim; ++i) {
        MPI_Offset tmp;
        ncmpi::inq_dimlen(ncid, dimids[i], &tmp);
        shape[i] = tmp;
        PRINT_ZERO("shape[%d]=%ld\n", i, (long)shape[i]);
    }
    if (first_only) {
        PRINT_ZERO("first_only shape[0]=%ld\n", (long)*(shape+1));
        g_a = NGA_Create64(C_FLOAT, ndim-1, shape+1, name, NULL);
    } else {
        g_a = NGA_Create64(C_FLOAT, ndim, shape, name, NULL);
    }
    PRINT_ZERO("read_data %s\n", name);
    read_data(g_a, ncid, varid, first_only);

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
    PRINT_ZERO(" after lgop buffsize=%ld\n", buffsize);
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
    int64_t extents_dims[2];
    int extents_ndim;
    int extents_type;
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
    MPI_Status recv_status;

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
        PRINT_ZERO("after lgop buffsize=%ld\n", buffsize);
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
    PRINT_ZERO("after lgop buffsize=%ld\n", buffsize);
    lower_received = new LatLonIdx[buffsize];
    upper_received = new LatLonIdx[buffsize];

    // First, ship the lowest left.
    send_to = me - 1;
    recv_from = me + 1;
    // All but first and last procs send and receive.
    if (me != firstproc && me != lastproc) {
        check(MPI_Sendrecv( &extras_lower.begin()->second[0],
                    extras_lower.begin()->second.size(), LatLonIdxType,
                    send_to, TAG_GHOST,
                    lower_received, buffsize, LatLonIdxType,
                    recv_from, TAG_GHOST, MPI_COMM_WORLD, &recv_status));
        check(MPI_Get_count(&recv_status,LatLonIdxType,&lower_received_count));
    }
    // Last proc sends left as well.
    if (me == lastproc) {
        check(MPI_Send(&extras_lower.begin()->second[0],
                extras_lower.begin()->second.size(), LatLonIdxType,
                send_to, TAG_GHOST, MPI_COMM_WORLD));
    }
    // First proc receives from right as well.
    if (me == firstproc) {
        check(MPI_Recv(lower_received, buffsize, LatLonIdxType,
                recv_from, TAG_GHOST, MPI_COMM_WORLD, &recv_status));
        check(MPI_Get_count(&recv_status,LatLonIdxType,&lower_received_count));
    }
    PRINT_ZERO("after sending left\n");

    // Next, ship the highest right.
    send_to = me + 1;
    recv_from = me - 1;
    // All but the first and last procs send and receive.
    if (me != firstproc && me != lastproc) {
        check(MPI_Sendrecv(&extras_upper.begin()->second[0],
                extras_upper.begin()->second.size(), LatLonIdxType,
                send_to, TAG_GHOST,
                upper_received, buffsize, LatLonIdxType,
                recv_from, TAG_GHOST, MPI_COMM_WORLD, &recv_status));
        check(MPI_Get_count(&recv_status,LatLonIdxType,&upper_received_count));
    }
    // Last proc receives from left as well.
    if (me == lastproc) {
        check(MPI_Recv(upper_received, buffsize, LatLonIdxType,
                recv_from, TAG_GHOST, MPI_COMM_WORLD, &recv_status));
        check(MPI_Get_count(&recv_status,LatLonIdxType,&upper_received_count));
    }
    // First proc sends right as well.
    if (me == firstproc) {
        check(MPI_Send(&extras_upper.begin()->second[0],
                extras_upper.begin()->second.size(), LatLonIdxType,
                send_to, TAG_GHOST, MPI_COMM_WORLD));
    }
    PRINT_ZERO("after sending right\n");

    // First, append received from right into last bin.
    if (me != lastproc) {
        it = --bin.end();
        it->second.insert(it->second.end(),
                lower_received, lower_received+lower_received_count);
    }
    PRINT_ZERO("after first insertions\n");
    // Second, insert received from left into first bin.
    if (me != firstproc) {
        it = bin.begin();
        it->second.insert(it->second.begin(),
                upper_received, upper_received+upper_received_count);
    }
    PRINT_ZERO("after second insertions\n");
    // Third, append local next-to-first.
    it = bin.begin();
    it_next = ++extras_lower.begin();
    if (it_next != extras_lower.end()) {
        it->second.insert(it->second.end(),
                it_next->second.begin(), it_next->second.end());
    }
    PRINT_ZERO("after third insertions\n");
    // Fourth, insert local next-to-last.
    it = --bin.end();
    it_prev = --extras_upper.end();
    if (it_prev != extras_upper.begin()) {
        --it_prev;
        it->second.insert(it->second.begin(),
                it_prev->second.begin(), it_prev->second.end());
    }
    PRINT_ZERO("after fourth insertions\n");
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
    PRINT_ZERO("after all insertions\n");

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
    PRINT_ZERO("BEGIN calc_weights\n");
    int binidx=-1;
    bin_t::const_iterator dst_bin_it = dst_bin.begin();
    bin_t::const_iterator dst_bin_end = dst_bin.end();
    int missing_src_bin = 0;
    int empty_src_bin = 0;
    int empty_dst_bin = 0;

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
            ++missing_src_bin;
            continue;
        } else if (src_bin_it->second.empty()) {
            ++empty_src_bin;
            continue;
        } else if (dst_bin_it->second.empty()) {
            ++empty_dst_bin;
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
    PRINT_SYNC("dst bins without src bins=%d\n", missing_src_bin);
    PRINT_SYNC("empty src bins=%d\n", empty_src_bin);
    PRINT_SYNC("empty dst bins=%d\n", empty_dst_bin);
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


/**
 * TODO
 *
 * Input weights and indices will have the same distributions.  Iterate over
 * the weights per processor and get/gather from the data array.  The data
 * array is assumed to have its first dimension as "cells" or something
 * similar e.g. "corners", "edges" and a second dimension indicating levels or
 * something similar.  This affects our get/gather call(s).
 */
static int interpolate(int g_data, int g_weights, int g_indices)
{
    int me = GA_Nodeid();

    int data_type;
    int data_ndim;
    int64_t data_shape[NC_MAX_VAR_DIMS];
    float *data;

    int weights_type;
    int weights_ndim;
    int64_t weights_lo[NC_MAX_VAR_DIMS];
    int64_t weights_hi[NC_MAX_VAR_DIMS];
    int64_t weights_ld[NC_MAX_VAR_DIMS];
    int64_t weights_shape[NC_MAX_VAR_DIMS];
    float *weights;

    int indices_type;
    int indices_ndim;
    int64_t indices_shape[NC_MAX_VAR_DIMS];
    int *indices;

    int g_result;
    int64_t results_shape[NC_MAX_VAR_DIMS];
    int64_t results_chunk[NC_MAX_VAR_DIMS];

    int64_t ignore_ld[NC_MAX_VAR_DIMS];
    int64_t get_lo[NC_MAX_VAR_DIMS];
    int64_t get_hi[NC_MAX_VAR_DIMS];
    int64_t acc_lo[NC_MAX_VAR_DIMS];
    int64_t acc_hi[NC_MAX_VAR_DIMS];
    int64_t acc_ld[NC_MAX_VAR_DIMS];
    int samples;
    float F_ZERO = 0;
    float F_ONE = 1;
    
    PRINT_ZERO("Begin interpolation\n");

    NGA_Inquire64(g_data, &data_type, &data_ndim, data_shape);
    NGA_Inquire64(g_weights, &weights_type, &weights_ndim, weights_shape);
    NGA_Inquire64(g_indices, &indices_type, &indices_ndim, indices_shape);
    NGA_Distribution64(g_weights, me, weights_lo, weights_hi);
    PRINT_ZERO("   data_shape={%ld,%ld}\n", data_shape[0], data_shape[1]);
    PRINT_ZERO("weights_shape={%ld,%ld}\n", weights_shape[0], weights_shape[1]);
    PRINT_ZERO("indices_shape={%ld,%ld}\n", indices_shape[0], indices_shape[1]);

    get_lo[1] = 0;
    get_hi[1] = data_shape[1]-1;
    acc_lo[1] = 0;
    acc_hi[1] = data_shape[1]-1;
    acc_ld[0] = data_shape[1];
    samples = weights_shape[1];

    results_shape[0] = weights_shape[0];
    results_shape[1] = data_shape[1];
    results_chunk[0] = 0;
    results_chunk[1] = results_shape[1];
    g_result = NGA_Create64(C_FLOAT, weights_ndim, results_shape,
            "results", results_chunk);
    GA_Fill(g_result, &F_ZERO);
    PRINT_ZERO("results_shape={%ld,%ld}\n", results_shape[0], results_shape[1]);

    if (0 > weights_lo[0] && 0 > weights_hi[0]) {
        // nothing to do on this proc
        return g_result;
    }

    PRINT_ZERO("before get loop\n");
    data = new float[data_shape[1]];
    NGA_Access64(g_weights, weights_lo, weights_hi, &weights, weights_ld);
    NGA_Access64(g_indices, weights_lo, weights_hi, &indices, weights_ld);
    for (int i=0,limit=weights_hi[0]-weights_lo[0]+1; i<limit; ++i) {
        bool missed_all = true;
        for (int j=0; j<samples; ++j) {
            int idx = indices[i*samples+j];
            get_lo[0] = idx;
            get_hi[0] = idx;
            acc_lo[0] = i+weights_lo[0];
            acc_hi[0] = i+weights_lo[0];
            if (0 > idx) {
                // negative index; don't get
                cerr << "[" << me << "] not getting " << i+weights_lo[0] << " because idx=" << idx << endl;
            } else {
                float weight = weights[i*samples+j];
                missed_all = false;
                NGA_Get64(g_data, get_lo, get_hi, data, ignore_ld);
                for (int k=0; k<data_shape[1]; ++k) {
                    data[k] *= weight;
                }
                NGA_Acc64(g_result, acc_lo, acc_hi, data, acc_ld, &F_ONE);
            }
        }
        if (missed_all) {
            cerr << "[" << me << "] MISSED ALL SAMPLES" << endl;
        }
    }
    NGA_Release64(g_weights, weights_lo, weights_hi);
    NGA_Release64(g_indices, weights_lo, weights_hi);
    delete [] data;

    GA_Sync();

    return g_result;
}


static void write_results(int g_results, int ncid_orig,
        const string& variablename, const string& filename)
{
    int me = GA_Nodeid();

    int results_type;
    int results_ndim;
    int64_t results_shape[NC_MAX_VAR_DIMS];
    int64_t results_lo[NC_MAX_VAR_DIMS];
    int64_t results_hi[NC_MAX_VAR_DIMS];
    int64_t results_ld[NC_MAX_VAR_DIMS];
    float *results = NULL;

    int ncid;
    MPI_Offset start[NC_MAX_VAR_DIMS];
    MPI_Offset count[NC_MAX_VAR_DIMS];
    MPI_Offset dims_shape[NC_MAX_VAR_DIMS];
    int dims[NC_MAX_VAR_DIMS];
    int varid;
    float ZERO = 0;

    NGA_Inquire64(g_results, &results_type, &results_ndim, results_shape);
    dims_shape[0] = NC_UNLIMITED;
    for (int i=1; i<results_ndim+1; ++i) {
        dims_shape[i] = results_shape[i-1];
    }
    PRINT_ZERO("dims_shape={%ld,%ld,%ld,%ld}\n",
            dims_shape[0], dims_shape[1], dims_shape[2], dims_shape[3]);

    ncmpi::create(MPI_COMM_WORLD, filename.c_str(),
            NC_64BIT_OFFSET, MPI_INFO_NULL, &ncid);
    ncmpi::def_dim(ncid, "time",       dims_shape[0], &dims[0]);
    PRINT_ZERO("      time=%d\n", dims[0]);
    ncmpi::def_dim(ncid, "cells",      dims_shape[1], &dims[1]);
    PRINT_ZERO("     cells=%d\n", dims[1]);
    ncmpi::def_dim(ncid, "interfaces", dims_shape[2], &dims[2]);
    PRINT_ZERO("interfaces=%d\n", dims[2]);
    ncmpi::def_var(ncid, variablename.c_str(), NC_FLOAT, 3, dims, &varid);
    PRINT_ZERO("       var=%d\n", dims[2]);
    ncmpi::put_att(ncid, varid, "missing_value", NC_FLOAT, 1, &ZERO);
    ncmpi::enddef(ncid);

    NGA_Distribution64(g_results, me, results_lo, results_hi);
    if (0 > results_lo[0] && 0 > results_hi[0]) {
        for (int i=0; i<results_ndim; ++i) {
            start[i] = 0;
            count[i] = 0;
        }
        ncmpi::put_vara_all(ncid, varid, start, count, results);
    } else {
        start[0] = 0;
        count[0] = 1;
        for (int i=1; i<results_ndim+1; ++i) {
            start[i] = results_lo[i-1];
            count[i] = results_hi[i-1]-results_lo[i-1]+1;
        }
        NGA_Access64(g_results, results_lo, results_hi, &results, results_ld);
        ncmpi::put_vara_all(ncid, varid, start, count, results);
        NGA_Release64(g_results, results_lo, results_hi);
    }

    ncmpi::close(ncid);
}
