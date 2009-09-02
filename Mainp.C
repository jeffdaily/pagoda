#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif // HAVE_CONFIG_H

// C includes, std and otherwise
#include <ga.h>
#include <macdecls.h>
#include <mpi.h>
#include <pnetcdf.h>
#include <unistd.h>

// C++ includes, std and otherwise
#include <algorithm>
#include <climits> // for INT_MAX
#include <cmath> // for M_PI
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

// C++ includes
#include "Common.H"
#include "LatLonBox.H"
#include "RangeException.H"
#include "Slice.H"
#include "SubsetterException.H"

using std::copy;
using std::cout;
using std::endl;
using std::fill;
using std::fixed;
using std::make_pair;
using std::map;
using std::multimap;
using std::ostringstream;
using std::setprecision;
using std::string;
using std::vector;

static double DEG2RAD = M_PI / 180.0;
//static double RAD2DEG = 180.0 / M_PI;
static int ZERO = 0;
static int64_t ZERO64 = 0;
static int ONE = 1;
static string COMPOSITE_PREFIX("GCRM_COMPOSITE");


#define ME GA_Nodeid()
int error;


//#define DEBUG
#ifdef DEBUG
#include <assert.h>
//#define DEBUG_MASKS
#define DEBUG_PRINT fprintf
#define DEBUG_PRINT_ME if (ME == 0) fprintf
#else
#define DEBUG_PRINT
#define DEBUG_PRINT_ME
#endif


#define MAX_NAME 80
char GOP_SUM[] = "+";
char NAME_VAR_IN[] = "var_in";
char NAME_VAR_OUT[] = "var_out";


#define ERR(e) { \
    ostringstream __os; \
        __os << "Error: " << e << endl; \
        throw SubsetterException(__os.str()); }

#define ERRNO(n) { \
    ostringstream __os; \
        __os << "Error: " << ncmpi_strerror(n) << endl; \
        throw SubsetterException(__os.str()); }

#define ERRNO_CHECK(n) \
        if (n != NC_NOERR) { \
            ERRNO(n); \
        }


#ifdef F77_DUMMY_MAIN
#  ifdef __cplusplus
extern "C"
#  endif
int F77_DUMMY_MAIN() { return 1; }
#endif


typedef struct {
    int ncid;          // id of source file
    int id;            // id within its source file
    string name;       //
    MPI_Offset size;   // MPI_Offset is the data type pnetcdf uses for sizes...
    bool is_unlimited; // whether this dimension is the record dimension
} DimInfo;
typedef map<string,DimInfo> DimInfoMap;
typedef vector<DimInfo> DimInfoVec;


typedef struct {
    int ncid;        // id of source file
    int id;          // id within its source file
    string name;     //
    int natt;        // number of attributes
    nc_type type;    // NC_BYTE, NC_CHAR, NC_SHORT, NC_INT, NC_FLOAT, NC_DOUBLE
    int ndim;        // number of dimensions within this variable
    DimInfoVec dims; // (ordered) vector of DimInfo instances
    vector<MPI_Offset> edges; // redundant data, but useful (dims[0].size, etc)
} VarInfo;
typedef map<string,VarInfo> VarInfoMap;
typedef vector<VarInfo> VarInfoVec;


typedef struct {
    int id;              // id of this file
    int ndim;            // number of dimensions
    int nvar;            // number of variables
    int natt;            // number of attributes
    int udim;            // the id of the unlimited (record) dimension
    DimInfoVec dims;     // (ordered) vector of DimInfo instances
    VarInfoVec vars;     // (ordered) vector of VarInfo instances
    DimInfoMap dims_map; // name->DimInfo mapping
    VarInfoMap vars_map; // name->VarInfo mapping
} FileInfo;


typedef struct {
    string name;        // usually just dim.name, but could be composite
    DimInfo dim;        // the DimInfo that this mask covers
    int64_t lo;         // index of local data's start in relation to global data
    int64_t hi;         // index of local data's end in relation to global data
    int64_t local_size; // number of mask bits owned by this process
    long local_count;   // how many "true" values owned by this process
    long global_count;  // how many "true" values total, on all processes
    int handle;         // the Global Arrays handle, if any
    int *data;          // the entire data for this mask, if any
    // NOTE: Each Mask will have either handle or data defined, but not both.
    // The rationale here is that a small dimension, such as
    // cell_corners (size=6), shouldn't be distributed in order
    // to avoid unnecessary small communication.
} Mask;
typedef map<string,Mask> MaskMap;
typedef vector<Mask> MaskVec;


typedef multimap<string,DimSlice> DimSliceMMap;
typedef map<int,int> IndexMap;


DimInfo create_dim_info(int ncid, int dimid);
VarInfo create_var_info(int ncid, int varid);
FileInfo create_file_info(int ncid);
int nc_to_mt(nc_type type);
MaskMap create_masks(DimInfoMap dims, DimSliceMMap slices);
void adjust_cell_masks(MaskMap &masks, LatLonBox box, FileInfo gridfile);
int* mask_get_data_local(Mask mask);
int* mask_get_data_all(Mask mask);
string composite_name(DimInfoVec dims);
string get_mask_name(DimInfoVec dims);
Mask create_composite_mask2d(MaskMap &masks, DimInfoVec dims);
Mask create_composite_mask3d(MaskMap &masks, DimInfoVec dims);
void create_composite_mask(MaskMap &masks, VarInfo varInfo);
void create_composite_masks(MaskMap &masks, VarInfoVec varInfos);
long count_mask(int g_mask);
void count_masks(MaskMap &masks);
void copy_record_var(VarInfo var_in, VarInfo var_out, MaskMap masks);
void copy_var(VarInfo var_in, VarInfo var_out, MaskMap masks, IndexMap *mapping);



int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();
    DEBUG_PRINT_ME(stderr, "After GA_Initialize\n");

    int ncid_in = -1;
    int ncid_grid = -1;
    int ncid_out = -1;

    FileInfo infile;
    FileInfo gridfile;

    // command line parameters
    string input_filename = "";
    string output_filename = "";
    string grid_filename = "";
    bool wants_region = false;
    LatLonBox box;
    DimSliceMMap slices; /* specified dimension subsetting */
    int c;
    opterr = 0;

    string usage = ""
        "Usage: subsetter [options] input_filename output_filename\n"
        "\n"
        "Generic options:\n"
        "-h                                 produce this usage statement\n"
        "-g path/to/gridfile.nc             points to external grid file\n"
        "-b box north,south,east,west       limits in degrees\n"
        "-d dimension,start[,stop[,step]]   as integer indices\n"
        ;

    try {

        ////////////////////////
        // parse command line
        ////////////////////////

        while ((c = getopt(argc, argv, "hb:g:d:")) != -1) {
            switch (c) {
                case 'h':
                    throw SubsetterException(usage);
                case 'b':
                    wants_region = true;
                    box = LatLonBox(optarg);
                    // HACK - we know our stuff is in radians
                    box.scale(DEG2RAD);
                    break;
                case 'g':
                    grid_filename = optarg;
                    break;
                case 'd':
                    try {
                        DimSlice slice(optarg);
                        slices.insert(make_pair(slice.get_name(),slice));
                    } catch (RangeException &ex) {
                        ERR("Bad dimension slice");
                    }
                    break;
                default:
                    ostringstream os;
                    os << "ERROR: Unrecognized argument '" << c << "'" << endl;
                    os << usage << endl;
                    throw SubsetterException(os.str());
            }
        }

        if (optind == argc) {
            ostringstream os;
            os << "ERROR: Input and output file arguments required" << endl;
            os << usage << endl;
            throw SubsetterException(os.str());
        } else if (optind+1 == argc) {
            ostringstream os;
            os << "ERROR: Output file argument required" << endl;
            os << usage << endl;
            throw SubsetterException(os.str());
        } else if (optind+2 == argc) {
            input_filename = argv[optind];
            output_filename = argv[optind+1];
        } else {
            ostringstream os;
            os << "ERROR: Too many files specified" << endl;
            os << usage << endl;
            throw SubsetterException(os.str());
        }

        // open our input file and gather its header info
        DEBUG_PRINT_ME(stderr, "Before first file open, attempting to open\n");
        DEBUG_PRINT_ME(stderr, "%s\n", input_filename.c_str());
        error = ncmpi_open(MPI_COMM_WORLD, input_filename.c_str(), NC_NOWRITE,
                MPI_INFO_NULL, &ncid_in);
        ERRNO_CHECK(error);
        infile = create_file_info(ncid_in);
        ncid_grid = ncid_in;
        gridfile = infile;

        // open our grid file and gather its header info, but only if one
        // was specified and that was different from the input file
        if (!grid_filename.empty() && grid_filename != input_filename) {
            DEBUG_PRINT_ME(stderr, "Before open grid file\n");
            error = ncmpi_open(MPI_COMM_WORLD, grid_filename.c_str(), NC_NOWRITE,
                    MPI_INFO_NULL, &ncid_grid);
            ERRNO_CHECK(error);
            gridfile = create_file_info(ncid_grid);
        }

        // Open the output file now.  Better now than after we do a bunch of work
        // only to find out this step failed.
        DEBUG_PRINT_ME(stderr, "Before output file open\n");
        error = ncmpi_create(MPI_COMM_WORLD, output_filename.c_str(), NC_WRITE,
                MPI_INFO_NULL, &ncid_out);
        ERRNO_CHECK(error);

        // Attempt to calculate the largest amount of memory needed per-process.
        // This has to do with our use of GA_Pack.  Each process needs roughly
        // at least as much memory as the largest variable in our input netcdf
        // files divided by the total number of processes.
        MPI_Offset max = 0;
        string max_name;
        for (VarInfoVec::iterator var=infile.vars.begin(); var!=infile.vars.end(); ++var) {
            MPI_Offset var_size = 1;
            for (DimInfoVec::iterator dim=var->dims.begin(); dim!=var->dims.end(); ++dim) {
                // TODO calculate the largest variable size (skipping record dimension)
                if (! dim->is_unlimited) {
                    var_size *= dim->size;
                }
            }
            if (var_size > max) {
                max = var_size;
                max_name = var->name;
            }
        }
        // MA_init values based on max size / #procs
        int64_t max_size = max / GA_Nnodes();
        DEBUG_PRINT_ME(stderr, "MA max memory %llu bytes\n", max_size*8);
        DEBUG_PRINT_ME(stderr, "MA max variable %s\n", max_name.c_str());
        if (MA_init(MT_DBL, max_size, max_size) == MA_FALSE) {
            char msg[] = "MA_init failed";
            GA_Error(msg, 0);
        };

        // Iterate over our input's dimensions and create masks based on them.
        // This simultaneously adjusts masks based on command-line params.
        MaskMap masks = create_masks(infile.dims_map, slices);

        if (wants_region) {
            // Adjust lat/lon (cells) mask based on the "box" cmd line param.
            // This simultaneously adjusts masks for corners and edges.
            adjust_cell_masks(masks, box, gridfile);
        }

        create_composite_masks(masks, infile.vars);
        create_composite_masks(masks, gridfile.vars);

#ifdef DEBUG_MASKS
        MaskMap::iterator test_it;
        for (test_it=masks.begin(); test_it!=masks.end(); ++test_it) {
            GA_Print(test_it->second);
        }
#endif // DEBUG_MASKS

        count_masks(masks);

        // Create the remappings for cells, corners, edges
        IndexMap cells_map;
        IndexMap corners_map;
        IndexMap edges_map;
        if (masks.find("cells") != masks.end()) {
            Mask &cells_mask = masks["cells"];
            // need to get entire mask
            int64_t lo = 0;
            int64_t hi = cells_mask.dim.size-1;
            //MPI_Offset hi = infile.dims_map["cells"].size-1;
            int mask[hi+1];
            NGA_Get64(cells_mask.handle, &lo, &hi, mask, NULL);
            for (int i=0,remap=-1; i<=hi; ++i) {
                if (mask[i] >= 1) {
                    cells_map[i] = ++remap;
                    //DEBUG_PRINT_ME(stderr, "cells_map[%d]=%d\n", i, cells_map[i]);
                }
            }
        }
        if (masks.find("corners") != masks.end()) {
            Mask &corners_mask = masks["corners"];
            // need to get entire mask
            int64_t lo = 0;
            int64_t hi = corners_mask.dim.size-1;
            int mask[hi+1];
            NGA_Get64(corners_mask.handle, &lo, &hi, mask, NULL);
            for (int i=0,remap=-1; i<=hi; ++i) {
                if (mask[i] >= 1) {
                    corners_map[i] = ++remap;
                    //DEBUG_PRINT_ME(stderr, "corners_map[%d]=%d\n", i, corners_map[i]);
                }
            }
        }
        if (masks.find("edges") != masks.end()) {
            Mask &edges_mask = masks["edges"];
            // need to get entire mask
            int64_t lo = 0;
            int64_t hi = edges_mask.dim.size-1;
            int mask[hi+1];
            NGA_Get64(edges_mask.handle, &lo, &hi, mask, NULL);
            for (int i=0,remap=-1; i<=hi; ++i) {
                if (mask[i] >= 1) {
                    edges_map[i] = ++remap;
                    //DEBUG_PRINT_ME(stderr, "edges_map[%d]=%d\n", i, edges_map[i]);
                }
            }
        }

        ///////////////////////////
        // define the output file
        ///////////////////////////

        // define dimensions, sized based on the masks we determined
        DimInfoMap out_dims;
        for (MaskMap::iterator it=masks.begin(); it!=masks.end(); ++it) {
            string name = it->first;
            if (name.find(COMPOSITE_PREFIX) == 0) continue; // skip composites
            Mask mask = it->second;
            MPI_Offset size = mask.global_count;
            DEBUG_PRINT_ME(stderr, "Defining dimension '%s'\n", name.c_str());
            if (mask.dim.is_unlimited) size = NC_UNLIMITED;
            int idp;
            error = ncmpi_def_dim(ncid_out, name.c_str(), size, &idp);
            ERRNO_CHECK(error);
            DimInfo dim;
            dim.ncid = ncid_out;
            dim.id = idp;
            dim.name = name;
            dim.size = size;
            dim.is_unlimited = mask.dim.is_unlimited;
            out_dims[name] = dim;
        }

        // copy global attributes
        for (int attidx=0, limit=infile.natt; attidx<limit; ++attidx) {
            char name[MAX_NAME];
            error = ncmpi_inq_attname(infile.id, NC_GLOBAL, attidx, name);
            ERRNO_CHECK(error);
            error = ncmpi_copy_att(infile.id, NC_GLOBAL, name, ncid_out, NC_GLOBAL);
            ERRNO_CHECK(error);
        }

        // define variables, copy their attributes
        VarInfoMap out_vars;
        for (VarInfoMap::iterator it=infile.vars_map.begin(); it!=infile.vars_map.end(); ++it) {
            int varidp;
            string var_name = it->first;
            VarInfo var_in = it->second;
            DEBUG_PRINT_ME(stderr, "Defining variable '%s' with dims ",var_name.c_str());
            int dims[var_in.ndim];
            bool skip = false;
            for (int dimidx=0; dimidx<var_in.ndim; ++dimidx) {
                // protect against degenerate dimensions (of zero size) that were 
                // removed during earlier processing
                skip |= (out_dims.find(var_in.dims[dimidx].name) == out_dims.end());
                if (skip) break;
                dims[dimidx] = out_dims[var_in.dims[dimidx].name].id;
                DEBUG_PRINT_ME(stderr, "%s,", var_in.dims[dimidx].name.c_str());
            }
            DEBUG_PRINT_ME(stderr, skip ? ", skipping due to degenerate dimension\n" : "\n");
            if (skip) continue; // see note above
            error = ncmpi_def_var(ncid_out, var_name.c_str(), var_in.type, var_in.ndim, dims, &varidp);
            ERRNO_CHECK(error);

            // copy attributes
            for (int attidx=0, limit=var_in.natt; attidx<limit; ++attidx) {
                char name[MAX_NAME];
                error = ncmpi_inq_attname(var_in.ncid, var_in.id, attidx, name);
                ERRNO_CHECK(error);
                error = ncmpi_copy_att(var_in.ncid, var_in.id, name, ncid_out, varidp);
                ERRNO_CHECK(error);
            }

            VarInfo var;
            var.ncid = ncid_out;
            var.id = varidp;
            var.name = var_name;
            var.natt = var_in.natt;
            var.type = var_in.type;
            var.ndim = var_in.ndim;
            for (int dimidx=0; dimidx<var_in.ndim; ++dimidx) {
                DimInfo dim = out_dims[var_in.dims[dimidx].name];
                var.dims.push_back(dim);
                var.edges.push_back(dim.size);
            }
            out_vars[var_name] = var;
        }

        error = ncmpi_enddef(ncid_out);
        ERRNO_CHECK(error);

        //////////////////////////////
        // write to the output file
        //////////////////////////////

        // copy data
        for (VarInfoMap::iterator it=out_vars.begin(); it!=out_vars.end(); ++it) {
            string name = it->first;
            VarInfo var_out = it->second;
            VarInfo var_in = infile.vars_map[name];
            bool is_cell_neighbors = (name == "cell_neighbors");
            bool is_cell_corners = (name == "cell_corners");
            bool is_cell_edges = (name == "cell_edges");
            if (var_out.dims[0].is_unlimited && var_out.dims.size() == 1) {
                // record coordinate variable
            } else if (var_out.dims[0].is_unlimited) {
                // record variable
                copy_record_var(var_in, var_out, masks);
            } else {
                // all other (non-record) variables
                if (is_cell_neighbors)    copy_var(var_in, var_out, masks,&cells_map);
                else if (is_cell_corners) copy_var(var_in, var_out, masks,&corners_map);
                else if (is_cell_edges)   copy_var(var_in, var_out, masks,&edges_map);
                else                      copy_var(var_in, var_out, masks,NULL);
            }

        }

        /////////////////////////////////////
        // Clean up.  Close all open files.
        /////////////////////////////////////

        error = ncmpi_close(ncid_in);
        ERRNO_CHECK(error);
        if (ncid_in != ncid_grid) {
            error = ncmpi_close(ncid_grid);
            ERRNO_CHECK(error);
        }
        error = ncmpi_close(ncid_out);
        ERRNO_CHECK(error);

    } catch (SubsetterException &ex) {
        if (ME == 0) cout << ex.what() << endl;
    }

    // Must always call these to exit cleanly.
    GA_Terminate();
    MPI_Finalize();

    return 0;
}


DimInfo create_dim_info(int ncid, int dimid)
{
    int udim;
    DimInfo dim;

    error = ncmpi_inq_unlimdim(ncid, &udim);
    ERRNO_CHECK(error);

    char name[MAX_NAME];
    error = ncmpi_inq_dim(ncid, dimid, name, &dim.size);
    ERRNO_CHECK(error);
    dim.ncid = ncid;
    dim.id = dimid;
    dim.name = string(name);
    dim.is_unlimited = (udim == dimid);

    return dim;
}


VarInfo create_var_info(int ncid, int varid)
{
    VarInfo var;
    int ndim;

    error = ncmpi_inq_ndims(ncid, &ndim);
    ERRNO_CHECK(error);

    char name[MAX_NAME];
    int dims[ndim]; // plenty big
    error = ncmpi_inq_var(ncid, varid, name, &var.type, &var.ndim, dims, &var.natt);
    ERRNO_CHECK(error);
    var.name = string(name);
    var.ncid = ncid;
    var.id = varid;
    for (int dimindex=0; dimindex<var.ndim; ++dimindex) {
        DimInfo dim = create_dim_info(ncid, dims[dimindex]);
        var.dims.push_back(dim);
        var.edges.push_back(dim.size);
    }

    return var;
}


FileInfo create_file_info(int ncid)
{
    FileInfo info;
    error = ncmpi_inq(ncid, &info.ndim, &info.nvar, &info.natt, &info.udim);
    ERRNO_CHECK(error);
    info.id = ncid;
    for (int idx=0; idx<info.ndim; ++idx) {
        DimInfo dim = create_dim_info(ncid, idx);
        info.dims.push_back(dim);
        info.dims_map.insert(make_pair(dim.name,dim));
    }
    for (int idx=0; idx<info.nvar; ++idx) {
        VarInfo var = create_var_info(ncid, idx);
        info.vars.push_back(var);
        info.vars_map.insert(make_pair(var.name,var));
    }
    return info;
}


int nc_to_mt(nc_type type)
{
    switch (type) {
        case NC_CHAR:
            return MT_CHAR;
        case NC_BYTE:
            break;
        case NC_SHORT:
            return MT_INT;
        case NC_INT:
            return MT_LONGINT;
        case NC_FLOAT:
            return MT_REAL;
        case NC_DOUBLE:
            return MT_DBL;
        default:
            break;
    }
    ERR("Unsupported data type");
}


MaskMap create_masks(DimInfoMap dims, DimSliceMMap slices)
{
    MaskMap masks;

    for (DimInfoMap::iterator dim=dims.begin(); dim!=dims.end(); ++dim) {
        string name = dim->first;
        DimInfo info = dim->second;
        int64_t size = info.size;
        if (size == 0) continue; // protect against a time.size == 0
        Mask mask;
        mask.dim = info;
        mask.name = info.name;
        if (GA_Nnodes() > size) {
            mask.handle = INT_MAX;
            mask.data = new int[size];
            mask.lo = 0;
            mask.hi = size - 1;
            if (slices.count(name) > 0) {
                // command line had a subset specified, so default the mask to false
                fill(mask.data, mask.data+size, ZERO);
            } else {
                // no subset on command line, so we want the entire dimension
                fill(mask.data, mask.data+size, ONE);
            }
        } else {
            mask.handle = NGA_Create64(MT_INT, 1, &size, (char*)name.c_str(), NULL);
            mask.data = NULL;
            NGA_Distribution64(mask.handle, ME, &mask.lo, &mask.hi);
            if (slices.count(name) > 0) {
                // command line had a subset specified, so default the mask to false
                GA_Fill(mask.handle, &ZERO);
            } else {
                // no subset on command line, so we want the entire dimension
                GA_Fill(mask.handle, &ONE);
            }
        }
        mask.local_size = mask.hi - mask.lo + 1;
        masks[name] = mask;
    }

    // Now we iterate over the slices and adjust the masks as needed.
    for (DimSliceMMap::iterator it=slices.begin(); it!=slices.end(); ++it) {
        DimSlice slice = it->second;
        string name = slice.get_name();
        MaskMap::iterator mask_iter = masks.find(name);
        if (mask_iter == masks.end()) {
            ERR("Sliced dimension '"+name+"' does not exist");
        }
        Mask &mask = mask_iter->second;
        int64_t size = dims[name].size;
        int64_t lo, hi, step;
        slice.indices(size, lo, hi, step);
        if (mask.data) {
            for (int idx=lo; idx<=hi; idx+=step) {
                mask.data[idx] = ONE;
            }
        } else {
            // determine which processes own the data specified by the slice
            // that way they can operate on their local portions
            int64_t mapping[2*1*GA_Nnodes()];
            int nproc, procs[GA_Nnodes()];
            nproc = NGA_Locate_region64(mask.handle, &lo, &hi, mapping, procs);
            for (int idx=0; idx<nproc; ++idx) {
                if (procs[idx] == ME) {
                    lo = mapping[idx*2*1];
                    hi = mapping[idx*2*1 + 1];
                    cout << ME << " owns " << lo << " to " << hi << endl;
                    // the lo to hi range might be strided; calculate the initial offset
                    int64_t offset = (lo - hi) % step;
                    offset = (offset == 0) ? 0 : step - offset;
                    lo += offset;
                    cout << ME << " offset=" << offset << ", new lo=" << lo << endl;
                    if (lo > hi) continue;
                    int *local;
                    NGA_Access64(mask.handle, &lo, &hi, &local, NULL);
                    for (int jdx=0; jdx<=(hi-lo); jdx+=step) {
                        cout << ME << " " << jdx+lo << endl;
                        local[jdx] = ONE;
                    }
                    NGA_Release_update64(mask.handle, &lo, &hi);
                }
            }
        }
    }

    return masks;
}


void adjust_cell_masks(MaskMap &masks, LatLonBox box, FileInfo gridfile)
{
    DEBUG_PRINT_ME(stderr, "adjust_cell_masks BEGIN\n");

    float *lon = NULL; // holds local grid_center_lon data, tmp
    float *lat = NULL; // holds local grid_center_lat data, tmp

    Mask cells_mask = masks["cells"]; // Mask object for cells
    NGA_Access64(cells_mask.handle, &cells_mask.lo, &cells_mask.hi,
            &cells_mask.data, NULL);

    // read local portion of the grid_center_lon variable
    MPI_Offset cells_mask_lo = cells_mask.lo;
    MPI_Offset cells_mask_local_size = cells_mask.local_size;
    lon = new float[cells_mask.local_size];
    error = ncmpi_get_vara_float_all(gridfile.id,
            gridfile.vars_map["grid_center_lon"].id,
            &cells_mask_lo, &cells_mask_local_size, &lon[0]);
    ERRNO_CHECK(error);

    // read local portion of the grid_center_lat variable
    lat = new float[cells_mask.local_size];
    error = ncmpi_get_vara_float_all(gridfile.id,
            gridfile.vars_map["grid_center_lat"].id,
            &cells_mask_lo, &cells_mask_local_size, &lat[0]);
    ERRNO_CHECK(error);

    // the actual cells mask construction
    for (int64_t cellidx=0; cellidx<cells_mask.local_size; ++cellidx) {
        cells_mask.data[cellidx] = box.contains(lat[cellidx],lon[cellidx]) ? 1 : 0;
    }

    // corners_mask, if available
    if (gridfile.vars_map.find("cell_corners") != gridfile.vars_map.end()
            && masks.find("corners") != masks.end()) {
        // retrieve the Mask, fill with zero
        Mask corners_mask = masks["corners"];
        GA_Fill(corners_mask.handle, &ZERO);

        // read local portion of cell_corners
        int corners_per_cell = gridfile.dims_map["cellcorners"].size;
        int local_size = cells_mask.local_size * corners_per_cell;
        MPI_Offset corners_lo[] = {cells_mask.lo, 0};
        MPI_Offset corners_count[] = {cells_mask.local_size, corners_per_cell};
        int *cell_corners = new int[local_size];
        error = ncmpi_get_vara_int_all(gridfile.id,
                gridfile.vars_map["cell_corners"].id,
                corners_lo, corners_count, &cell_corners[0]);
        ERRNO_CHECK(error);

        // Create mask over local portion of cell_corners.
        // The following scatter accumulate expects the index array to be int**
        // so create that array while we're at it...
        int *local_mask = new int[local_size];
        int64_t **subs = new int64_t*[local_size];
        size_t local_index = 0;
        for (int64_t cellidx=0; cellidx<cells_mask.local_size; ++cellidx) {
            for (int corneridx=0; corneridx<corners_per_cell; ++corneridx) {
                subs[local_index] = new int64_t[1];
                subs[local_index][0] = cell_corners[local_index];
                local_mask[local_index++] = cells_mask.data[cellidx];
            }
        }

        // Scatter/accumulate the local_mask into the corners_mask
        NGA_Scatter_acc64(corners_mask.handle, local_mask, subs, local_size, &ONE);

        // Clean up!
        delete [] cell_corners;
        for (int64_t idx=0; idx<local_size; ++idx) {
            delete [] subs[idx];
        }
        delete [] subs;
    }

    // edges_mask, if available
    if (gridfile.vars_map.find("cell_edges") != gridfile.vars_map.end()
            && masks.find("edges") != masks.end()) {
        // retrieve the Mask, fill with zero
        Mask edges_mask = masks["edges"];
        GA_Fill(edges_mask.handle, &ZERO);

        // read local portion of cell_edges
        int edges_per_cell = gridfile.dims_map["celledges"].size;
        int local_size = cells_mask.local_size * edges_per_cell;
        MPI_Offset edges_lo[] = {cells_mask.lo, 0};
        MPI_Offset edges_count[] = {cells_mask.local_size, edges_per_cell};
        int *cell_edges = new int[local_size];
        error = ncmpi_get_vara_int_all(gridfile.id,
                gridfile.vars_map["cell_edges"].id,
                edges_lo, edges_count, &cell_edges[0]);
        ERRNO_CHECK(error);

        // Create mask over local portion of cell_edges.
        // The following scatter accumulate expects the index array to be int**
        // so create that array while we're at it...
        int *local_mask = new int[local_size];
        int64_t **subs = new int64_t*[local_size];
        size_t local_index = 0;
        for (int64_t cellidx=0; cellidx<cells_mask.local_size; ++cellidx) {
            for (int edgeidx=0; edgeidx<edges_per_cell; ++edgeidx) {
                subs[local_index] = new int64_t[1];
                subs[local_index][0] = cell_edges[local_index];
                local_mask[local_index++] = cells_mask.data[cellidx];
            }
        }

        // Scatter/accumulate the local_mask into the edges_mask
        NGA_Scatter_acc64(edges_mask.handle, local_mask, subs, local_size, &ONE);

        // Clean up!
        delete [] cell_edges;
        for (int64_t idx=0; idx<local_size; ++idx) {
            delete [] subs[idx];
        }
        delete [] subs;
    }

    // clean up
    NGA_Release_update64(cells_mask.handle, &cells_mask.lo, &cells_mask.hi);
    cells_mask.data = NULL;
    delete [] lon;
    delete [] lat;

    DEBUG_PRINT_ME(stderr, "adjust_cell_masks END\n");
}


int* mask_get_data_local(Mask mask)
{
    if (mask.data) {
        return mask.data;
    } else {
        int *data;
        NGA_Access64(mask.handle, &mask.lo, &mask.hi, &data, NULL);
        return data;
    }
}


int* mask_get_data_all(Mask mask)
{
    int64_t size = mask.dim.size;
    int *data = new int[size];
    if (mask.data) {
        copy(mask.data, mask.data+size, data);
    } else {
        int64_t hi = size - 1;
        NGA_Get64(mask.handle, &ZERO64, &hi, data, NULL);
    }
    return data;
}


string composite_name(DimInfoVec dims)
{
    string name = COMPOSITE_PREFIX;
    size_t idx = 0;
    if (dims[0].is_unlimited) idx = 1;
    for (; idx<dims.size(); ++idx) {
        name += '_' + dims[idx].name;
    }
    return name;
}


string get_mask_name(DimInfoVec dims)
{
    if (dims.size() == 1) {
        return dims[0].name;
    } else {
        return composite_name(dims);
    }
}


Mask create_composite_mask2d(MaskMap &masks, DimInfoVec dims)
{
    DEBUG_PRINT_ME(stderr, "create_composite_mask2d\n");

    // create and access the new composite mask
    string name = composite_name(dims);
    int64_t size = dims[0].size * dims[1].size;
    int64_t chunk = dims[1].size;
    int g_the_mask = NGA_Create64(MT_INT, 1, &size, (char*)name.c_str(), &chunk);
    int64_t the_mask_lo;
    int64_t the_mask_hi;
    int *the_mask;
    NGA_Distribution64(g_the_mask, ME, &the_mask_lo, &the_mask_hi);
    NGA_Access64(g_the_mask, &the_mask_lo, &the_mask_hi, &the_mask, NULL);

    // get the data for the first dimension, but only the data we need
    // since this first mask dimension corresponds to what we accessed above
    int64_t mask1_lo = the_mask_lo / dims[1].size;
    int64_t mask1_hi = (the_mask_hi - dims[1].size + 1) / dims[1].size;
    int64_t mask1_size = mask1_hi - mask1_lo + 1;
    int *mask1 = new int[mask1_size];
    NGA_Get64(masks[dims[0].name].handle, &mask1_lo, &mask1_hi, mask1, NULL);

    // get the entire mask data for the second dimension
    MPI_Offset mask2_size = dims[1].size;
    int *mask2 = mask_get_data_all(masks[dims[1].name]);

    // iterate over the dimensions, writing values into the composite mask
    // based on the dimension masks we have
    for (int64_t idx=0; idx<mask1_size; ++idx) {
        if (mask1[idx] > 0) {
            for (MPI_Offset jdx=0; jdx<mask2_size; ++jdx) {
                the_mask[idx*mask2_size + jdx] = mask2[jdx];
            }
        } else {
            for (MPI_Offset jdx=0; jdx<mask2_size; ++jdx) {
                the_mask[idx*mask2_size + jdx] = 0;
            }
        }
    }

    NGA_Release_update64(g_the_mask, &the_mask_lo, &the_mask_hi);
    delete [] mask1;
    delete [] mask2;

    Mask mask;
    mask.name = name;
    mask.data = NULL;
    mask.handle = g_the_mask;
    mask.lo = the_mask_lo;
    mask.hi = the_mask_hi;
    //mask.local_size;   // not important; composite masks aren't written to file
    //mask.local_count;  // not important; composite masks aren't written to file
    //mask.global_count; // not important; composite masks aren't written to file

    return mask;
}


Mask create_composite_mask3d(MaskMap &masks, DimInfoVec dims)
{
    throw SubsetterException("create_composite_mask3d NOT IMPLEMENTED");
    /*
       string name = composite_name(dims);
       MPI_Offset size = dims[0].size * dims[1].size * dims[2].size;
       MPI_Offset chunk = dims[1].size * dims[2].size;
       int g_a = NGA_Create64(MT_INT, 1, &size, (char*)name.c_str(), &chunk);
       return g_a;
       */
}


void create_composite_mask(MaskMap &masks, VarInfo varInfo)
{
    DimInfoVec dims = varInfo.dims;
    if (dims.size() > 0 && dims[0].name == "time") {
        DEBUG_PRINT_ME(stderr, "erasing time dimension from var info\n");
        dims.erase(dims.begin());
    }
    if (dims.size() == 0) {
        DEBUG_PRINT_ME(stderr, "dims.size() == 0, skipping\n");
    } else if (dims.size() == 1) {
        DEBUG_PRINT_ME(stderr, "dims.size() == 1, skipping\n");
    } else if (dims.size() == 2) {
        string name = composite_name(dims);
        if (masks.find(name) != masks.end()) {
            DEBUG_PRINT_ME(stderr, "dims.size() == 2, name=%s, skipping\n", name.c_str());
            return;
        } else {
            DEBUG_PRINT_ME(stderr, "dims.size() == 2, name=%s\n", name.c_str());
            masks[name] = create_composite_mask2d(masks, dims);
        }
    } else if (dims.size() == 3) {
        string name = composite_name(dims);
        if (masks.find(name) != masks.end()) {
            DEBUG_PRINT_ME(stderr, "dims.size() == 3, name=%s, skipping\n", name.c_str());
            return;
        } else {
            DEBUG_PRINT_ME(stderr, "dims.size() == 3, name=%s\n", name.c_str());
            masks[name] = create_composite_mask3d(masks, dims);
        }
    } else {
        ERR("bad dimension size (must be <= 3)");
    }
}


void create_composite_masks(MaskMap &masks, VarInfoVec varInfos)
{
    DEBUG_PRINT_ME(stderr, "create_composite_masks size=%zu\n", varInfos.size());
    for (size_t idx=0; idx<varInfos.size(); ++idx) {
        VarInfo varInfo = varInfos[idx];
        create_composite_mask(masks, varInfo);
    }
}


void count_masks(MaskMap &masks)
{
    DEBUG_PRINT_ME(stderr, "count_masks BEGIN\n");

    for (MaskMap::iterator it=masks.begin(); it!=masks.end(); ++it) {
        if (it->first.find(COMPOSITE_PREFIX) == 0) continue; // don't count these
        Mask &mask = it->second;
        mask.local_count = 0;
        mask.global_count = 0;
        if (mask.data) {
            for (size_t i=0; i<mask.local_size; ++i) {
                if (mask.data[i] != 0) ++mask.local_count;
            }
            mask.global_count = mask.local_count;
        } else {
            NGA_Access64(mask.handle, &mask.lo, &mask.hi, &mask.data, NULL);
            for (size_t i=0; i<mask.local_size; ++i) {
                if (mask.data[i] != 0) ++mask.local_count;
            }
            NGA_Release_update64(mask.handle, &mask.lo, &mask.hi);
            mask.data = NULL;
            mask.global_count = mask.local_count;
            GA_Lgop(&mask.global_count, 1, GOP_SUM);
        }
    }

    DEBUG_PRINT_ME(stderr, "count_masks END\n");
}


// record variables
void copy_record_var(
        VarInfo var_in,
        VarInfo var_out,
        MaskMap masks)
{
    Mask the_record_mask = masks["time"];
    int num_records = the_record_mask.dim.size;
    int *record_mask = mask_get_data_all(the_record_mask);
    string name = var_in.name;
    int ndim = var_in.ndim;
    int64_t chunk_in, chunk_out;
    int64_t lo_in, lo_out;
    int64_t hi_in, hi_out;
    MPI_Offset start_in[ndim], start_out[ndim];
    MPI_Offset count_in[ndim], count_out[ndim];
    int64_t size_in, size_out;
    // Recall we're linearizing these arrays so that GA_Pack works.
    // Due to the linearization, we must specify a chunk size that equates
    // to the product of all but the first dim, otherwise we'd get unpredictable 
    // array distribution (across otherwise multi-dimensional bounds)
    size_in  =  var_in.edges[1]; // start at dim after record dim
    size_out = var_out.edges[1]; // start at dim after record dim
    chunk_in  = 1;
    chunk_out = 1;
    for (int idx=2; idx<ndim; ++idx) {
        size_in  *=  var_in.edges[idx];
        size_out *= var_out.edges[idx];
        chunk_in  *=   var_in.edges[idx];
        chunk_out *=  var_out.edges[idx];
    }
    int g_var_in = NGA_Create64(nc_to_mt(var_in.type), 1, &size_in, NAME_VAR_IN, &chunk_in);
    // we must now translate from this linearized representation to the
    // start/count representation expected by pnetcdf
    NGA_Distribution64(g_var_in, ME, &lo_in, &hi_in);
    start_in[0] = 0;                               // start at first record
    start_in[1] = lo_in / chunk_in;                // again, after record dim
    count_in[0] = 1;                               // read single record
    count_in[1] = (hi_in - lo_in + 1) / chunk_in;  // again, after record dim
    for (int idx=2; idx<ndim; ++idx) {
        start_in[idx] = 0;
        count_in[idx] = var_in.edges[idx];           // read entire dims thereafter
    }
    int g_var_out = NGA_Create64(nc_to_mt(var_in.type), 1, &size_out, NAME_VAR_OUT, &chunk_out);
    NGA_Distribution64(g_var_out, ME, &lo_out, &hi_out);
    start_out[0] = 0;                                 // start at first record
    start_out[1] = lo_out / chunk_out;                // again, after record dim
    count_out[0] = 1;                                 // write single record
    count_out[1] = (hi_out - lo_out + 1) / chunk_out; // again, after record dim
    for (int idx=2; idx<ndim; ++idx) {
        start_out[idx] = 0;
        count_out[idx] = var_out.edges[idx]; // write entire dims thereafter
    }
#ifdef DEBUG
    // each proc takes turns with output so get ordered output
    for (int who=0; who<GA_Nnodes(); ++who) {
        if (who == ME) {
            if (ME == 0) {
                fprintf(stderr, "======= %s ==== copy_record_var\n", name.c_str());
            }
            fprintf(stderr, "[%d] chunk_in=%lld\n", ME, chunk_in);
            fprintf(stderr, "[%d] lo_in=%lld\n", ME, lo_in);
            fprintf(stderr, "[%d] hi_in=%lld\n", ME, hi_in);
            fprintf(stderr, "[%d] size_in=%lld\n", ME, size_in);
            fprintf(stderr, "[%d] start_in=(", ME);
            for (int idx=0; idx<ndim; ++idx) {
                fprintf(stderr, "%lld,", start_in[idx]);
            }
            fprintf(stderr, ")\n");
            fprintf(stderr, "[%d] count_in=(", ME);
            for (int idx=0; idx<ndim; ++idx) {
                fprintf(stderr, "%lld,", count_in[idx]);
            }
            fprintf(stderr, ")\n");
            fflush(stderr);
        }
        GA_Sync();
    }
#endif
    for (MPI_Offset record=0; record<num_records; ++record) {
        if (record_mask[record] == 0) continue; // skip if masked out

        // set the record to read
        start_in[0] = record;

        // read from file to local memory
        // NOTE: data type dependent
        if (var_out.type == NC_INT) {
            int *ptr;
            NGA_Access64(g_var_in, &lo_in, &hi_in, &ptr, NULL);
            error = ncmpi_get_vara_int_all(var_in.ncid, var_in.id, start_in, count_in, ptr);
        } else if (var_out.type == NC_FLOAT) {
            float *ptr;
            NGA_Access64(g_var_in, &lo_in, &hi_in, &ptr, NULL);
            error = ncmpi_get_vara_float_all(var_in.ncid, var_in.id, start_in, count_in, ptr);
        } else if (var_out.type == NC_DOUBLE) {
            double *ptr;
            NGA_Access64(g_var_in, &lo_in, &hi_in, &ptr, NULL);
            error = ncmpi_get_vara_double_all(var_in.ncid, var_in.id, start_in, count_in, ptr);
        }
        ERRNO_CHECK(error);
        NGA_Release_update64(g_var_in, &lo_in, &hi_in);

        // locate the appropriate mask handle and call GA_Pack
        // NOTE: does not depend on data type dependent call
        string mask_name = get_mask_name(var_in.dims);
        if (masks.find(mask_name) == masks.end()) {
            ERR("Unable to locate composite mask");
        }
        int64_t icount;
        DEBUG_PRINT_ME(stderr, "before pack\n");
        GA_Pack64(g_var_in, g_var_out, masks[mask_name].handle, 0, size_in-1, &icount);
        DEBUG_PRINT_ME(stderr, "after pack, icount=%lld\n", icount);

        // NO REMAPPING

        DEBUG_PRINT_ME(stderr, "WRITING: %s\n", name.c_str());

        // Write to file from local memory
        // NOTE: data type dependent call
        if (var_out.type == NC_INT) {
            int *ptr;
            NGA_Access64(g_var_out, &lo_out, &hi_out, &ptr, NULL);
            error = ncmpi_put_vara_int_all(var_out.ncid, var_out.id, start_out, count_out, &ptr[0]);
        } else if (var_out.type == NC_FLOAT) {
            float *ptr;
            NGA_Access64(g_var_out, &lo_out, &hi_out, &ptr, NULL);
            error = ncmpi_put_vara_float_all(var_out.ncid, var_out.id, start_out, count_out, &ptr[0]);
        } else if (var_out.type == NC_DOUBLE) {
            double *ptr;
            NGA_Access64(g_var_out, &lo_out, &hi_out, &ptr, NULL);
            error = ncmpi_put_vara_double_all(var_out.ncid, var_out.id, start_out, count_out, &ptr[0]);
        }
        ERRNO_CHECK(error);
        NGA_Release_update64(g_var_out, &lo_out, &hi_out);
        ++start_out[0]; // increment record to write
    }

    GA_Destroy(g_var_in);
    GA_Destroy(g_var_out);
}


// non-record variables
void copy_var(VarInfo var_in, VarInfo var_out, MaskMap masks, IndexMap *mapping)
{
    string name = var_in.name;
    int ndim = var_in.ndim;
    int64_t chunk_in, chunk_out;
    int64_t lo_in, lo_out;
    int64_t hi_in, hi_out;
    MPI_Offset start_in[ndim], start_out[ndim];
    MPI_Offset count_in[ndim], count_out[ndim];
    int64_t size_in, size_out;
    // Recall we're linearizing these arrays so that GA_Pack works.
    // Due to the linearization, we must specify a chunk size that equates
    // to all but the first dim, otherwise we'd get unpredictable 
    // array distribution (across otherwise multi-dimensional bounds)
    size_in  =  var_in.edges[0];
    size_out = var_out.edges[0];
    chunk_in  = 1;
    chunk_out = 1;
    for (int idx=1; idx<ndim; ++idx) {
        size_in  *=  var_in.edges[idx];
        size_out *= var_out.edges[idx];
        chunk_in  *=   var_in.edges[idx];
        chunk_out *=  var_out.edges[idx];
    }
    int g_var_in  = NGA_Create64(nc_to_mt(var_in.type), 1, &size_in,  NAME_VAR_IN,  &chunk_in);
    int g_var_out = NGA_Create64(nc_to_mt(var_in.type), 1, &size_out, NAME_VAR_OUT, &chunk_out);
    // we must now translate from this linearized representation to the
    // start/count representation expected by pnetcdf
    NGA_Distribution64(g_var_in, ME, &lo_in, &hi_in);
    start_in[0] = lo_in / chunk_in;
    count_in[0] = (hi_in - lo_in + 1) / chunk_in;
    for (int idx=1; idx<ndim; ++idx) {
        start_in[idx] = 0;
        count_in[idx] = var_in.edges[idx];
    }
    NGA_Distribution64(g_var_out, ME, &lo_out, &hi_out);
    start_out[0] = lo_out / chunk_out;
    count_out[0] = (hi_out - lo_out + 1) / chunk_out;
    for (int idx=1; idx<ndim; ++idx) {
        start_out[idx] = 0;
        count_out[idx] = var_out.edges[idx];
    }
#ifdef DEBUG
    // each proc takes turns with output so get ordered output
    for (int who=0; who<GA_Nnodes(); ++who) {
        if (who == ME) {
            if (ME == 0) {
                fprintf(stderr, "======= %s ==== copy_var\n", name.c_str());
            }
            fprintf(stderr, "[%d] chunk_in=%lld\n", ME, chunk_in);
            fprintf(stderr, "[%d] lo_in=%lld\n", ME, lo_in);
            fprintf(stderr, "[%d] hi_in=%lld\n", ME, hi_in);
            fprintf(stderr, "[%d] size_in=%lld\n", ME, size_in);
            fprintf(stderr, "[%d] start_in=(", ME);
            for (int idx=0; idx<ndim; ++idx) {
                fprintf(stderr, "%lld,", start_in[idx]);
            }
            fprintf(stderr, ")\n");
            fprintf(stderr, "[%d] count_in=(", ME);
            for (int idx=0; idx<ndim; ++idx) {
                fprintf(stderr, "%lld,", count_in[idx]);
            }
            fprintf(stderr, ")\n");
            fflush(stderr);
        }
        GA_Sync();
    }
#endif
    // read from file to local memory
    // NOTE: data type dependent
    if (var_out.type == NC_INT) {
        int *ptr;
        NGA_Access64(g_var_in, &lo_in, &hi_in, &ptr, NULL);
        error = ncmpi_get_vara_int_all(var_in.ncid, var_in.id, start_in, count_in, ptr);
    } else if (var_out.type == NC_FLOAT) {
        float *ptr;
        NGA_Access64(g_var_in, &lo_in, &hi_in, &ptr, NULL);
        error = ncmpi_get_vara_float_all(var_in.ncid, var_in.id, start_in, count_in, ptr);
    } else if (var_out.type == NC_DOUBLE) {
        double *ptr;
        NGA_Access64(g_var_in, &lo_in, &hi_in, &ptr, NULL);
        error = ncmpi_get_vara_double_all(var_in.ncid, var_in.id, start_in, count_in, ptr);
    }
    ERRNO_CHECK(error);
    NGA_Release_update64(g_var_in, &lo_in, &hi_in);

    // locate the appropriate mask handle and call GA_Pack
    // NOTE: does not depend on data type dependent call
    string mask_name = get_mask_name(var_in.dims);
    if (masks.find(mask_name) == masks.end()) {
        ERR("Unable to locate composite mask");
    }
    int64_t icount;
    DEBUG_PRINT_ME(stderr, "before pack\n");
    GA_Pack64(g_var_in, g_var_out, masks[mask_name].handle, 0, size_in-1, &icount);
    DEBUG_PRINT_ME(stderr, "after pack, icount=%lld\n", icount);

    // If we have one of the special mapping variables, perform the
    // remapping now.
    if (mapping) {
        DEBUG_PRINT_ME(stderr, "MAPPING: %s\n", name.c_str());
        int *ptr;
        NGA_Access64(g_var_out, &lo_out, &hi_out, &ptr, NULL);
        MPI_Offset limit = hi_out - lo_out + 1;
        for (size_t idx=0; idx<limit; ++idx) {
            if (mapping->find(ptr[idx]) == mapping->end()) {
                ptr[idx] = -1;
            } else {
                ptr[idx] = (*mapping)[ptr[idx]];
            }
        }
        NGA_Release_update64(g_var_out, &lo_out, &hi_out);
    }

    DEBUG_PRINT_ME(stderr, "WRITING: %s\n", name.c_str());

    // Write to file from local memory
    // NOTE: data type dependent call
    if (var_out.type == NC_INT) {
        int *ptr;
        NGA_Access64(g_var_out, &lo_out, &hi_out, &ptr, NULL);
        error = ncmpi_put_vara_int_all(var_out.ncid, var_out.id, start_out, count_out, &ptr[0]);
    } else if (var_out.type == NC_FLOAT) {
        float *ptr;
        NGA_Access64(g_var_out, &lo_out, &hi_out, &ptr, NULL);
        error = ncmpi_put_vara_float_all(var_out.ncid, var_out.id, start_out, count_out, &ptr[0]);
    } else if (var_out.type == NC_DOUBLE) {
        double *ptr;
        NGA_Access64(g_var_out, &lo_out, &hi_out, &ptr, NULL);
        error = ncmpi_put_vara_double_all(var_out.ncid, var_out.id, start_out, count_out, &ptr[0]);
    }
    ERRNO_CHECK(error);
    NGA_Release_update64(g_var_out, &lo_out, &hi_out);

    GA_Destroy(g_var_in);
    GA_Destroy(g_var_out);
}

