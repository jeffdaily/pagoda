#include <algorithm>
  using std::count;
  using std::fill;
#include <iostream>
  using std::cout;
  using std::endl;
#include <map>
  using std::map;
#include <math.h>
#include <set>
  using std::set;
#include <stdlib.h>
#include <string>
  using std::string;
#include <unistd.h>
#include <vector>
  using std::vector;

#include <netcdfcpp.h>

#include "DimensionSlice.H"
#include "LatLonBox.H"
#include "RangeException.H"

double DEG2RAD = M_PI / 180.0;
double RAD2DEG = 180.0 / M_PI;

// Create this global NcError instance so that netcdf errors are nonfatal.
//NcError ERROR(NcError::verbose_nonfatal);


typedef map<string,NcDim*> DimMap;
typedef map<string,NcVar*> VarMap;
typedef map<string,int> VarParentIdMap;
typedef map<string,bool*> MaskMap;
typedef map<string,long> DimSizeMap;
typedef vector<DimensionSlice> DimSlices;

// used for reading in variables from files
// they read the entire contents of the variable
// the masked read will return a buffer that is the size of the packed/subset
// variable being read.
template <class T> T* read_var(NcVar *var);
template <class T> T* read_var(NcVar *var, bool **masks, long *packed_sizes);
template <class T> void _read_var_recursive(T *out, long& out_idx,
        T *in, long &in_idx, bool **masks, long *edges, long *strides,
        int num_dims, int depth);

// these functions create the dimension masks used later and also
// adjust the masks based on the command-line input
void create_masks(MaskMap &masks, DimMap &dims);
void adjust_masks(MaskMap &masks, DimMap &dims, DimSlices slices);
void adjust_cells_mask(MaskMap &masks, DimMap &dims, LatLonBox box,
                       NcFile *gridfile);

// these functions copy attributes, dimensions, and variable sizes
// which means the output file is in define mode
void copy_global_attributes(NcFile *outfile, NcFile *infile, NcFile *gridfile);
DimMap add_dims(NcFile *outfile, DimSizeMap sizes);
VarMap add_vars(NcFile *outfile, DimMap out_dims, VarMap in_vars,
                VarParentIdMap in_var_parents);

// high level function for copying data between files while respecting masks
void copy_variable(NcVar *in, NcVar *out, bool **masks, long *packed_sizes,
                   int *remap = NULL);
template <class T> void _copy_variable(NcVar *in, NcVar *out, bool **masks,
                                       long *packed_sizes);
void _copy_variable_int(NcVar *in, NcVar *out, bool **masks,
                        long *packed_sizes, int *remap = NULL);


int main(int argc, char **argv)
{
  string usage = ""
"Usage: subsetter [options] input_filename output_filename\n"
"\n"
"Generic options:\n"
"-h                                 produce this usage statement\n"
"-g path/to/gridfile.nc             points to external grid file\n"
"-b box north,south,east,west       limits in degrees\n"
"-d dimension,start[,stop[,step]]   as integer indices\n"
;

  NcFile *infile = NULL;
  NcFile *gridfile = NULL;
  NcFile *outfile = NULL;
  DimSlices slices; /* command line specified subsetting */
  VarMap in_vars;
  DimMap in_dims;
  VarParentIdMap in_var_parents;
  VarMap out_vars;
  DimMap out_dims;
  MaskMap masks; /* dim name --> mask array */
  DimSizeMap out_dim_sizes; /* dim name --> packed mask array length */
  int *cells_map = NULL;
  int *corners_map = NULL;
  int *edges_map = NULL;

  // command line parameters
  string input_filename = "";
  string output_filename = "";
  string grid_filename = "";
  bool wants_region = false;
  LatLonBox box;
  int c;
  opterr = 0;

  ////////////////////////
  // parse command line
  ////////////////////////

  while ((c = getopt(argc, argv, "hb:g:d:")) != -1) {
    switch (c) {
      case 'h':
        cout << usage << endl;
        exit(EXIT_SUCCESS);
      case 'b':
        wants_region = true;
        box = LatLonBox(optarg);
        break;
      case 'g':
        grid_filename = optarg;
        break;
      case 'd':
        try {
        slices.push_back(DimensionSlice(optarg));
        } catch (RangeException &ex) {
          cout << "ERROR: Bad dimesion slice '" << optarg << "'" << endl;
          exit(EXIT_FAILURE);
        }
        break;
      default:
        cout << "ERROR: Unrecognized argument '" << c << "'" << endl;
        cout << usage << endl;
        exit(EXIT_FAILURE);
    }
  }

  if (optind == argc) {
    cout << "ERROR: Input and output file arguments required" << endl;
    cout << usage << endl;
    exit(EXIT_FAILURE);
  } else if (optind+1 == argc) {
    cout << "ERROR: Output file argument required" << endl;
    cout << usage << endl;
    exit(EXIT_FAILURE);
  } else if (optind+2 == argc) {
    input_filename = argv[optind];
    output_filename = argv[optind+1];
  } else {
    cout << "ERROR: Too many files specified" << endl;
    cout << usage << endl;
    exit(EXIT_FAILURE);
  }

  ////////////////////////
  // begin the real work
  ////////////////////////

  // open our file(s)
  infile = new NcFile(input_filename.c_str(), NcFile::ReadOnly, NULL, 0,
                      NcFile::Offset64Bits);
  if (!infile->is_valid()) {
    cout << "Couldn't open file '" << input_filename << "'" << endl;
    exit(EXIT_FAILURE);
  }
  if (!grid_filename.empty() && grid_filename != input_filename) {
    gridfile = new NcFile(grid_filename.c_str(), NcFile::ReadOnly, NULL, 0,
                          NcFile::Offset64Bits);
    if (!gridfile->is_valid()) {
      cout << "Couldn't open grid file '" << grid_filename << "'" << endl;
      exit(EXIT_FAILURE);
    }
  } else {
    gridfile = infile;
  }
  // Open the output file now.  Better now than after we do a bunch of work
  // only to find out this step failed. Also, set it to NoFill to optimize
  // writes.
  outfile = new NcFile(output_filename.c_str(), NcFile::New, NULL, 0,
                       NcFile::Offset64Bits);
  if (!outfile->is_valid()) {
    cout << "Could not create output file '" << output_filename << "'" << endl;
    exit(EXIT_FAILURE);
  }
  outfile->set_fill(NcFile::NoFill);

  // Gather dimensions from the input and grid files.
  // The dimensions from the grid file are read last and overwrite anything
  // found in the input file.
  for (int dimidx=0, limit=infile->num_dims(); dimidx<limit; ++dimidx) {
    NcDim *dim = infile->get_dim(dimidx);
    in_dims[dim->name()] = dim;
  }
  if (infile != gridfile) { // small optimization
    for (int dimidx=0, limit=gridfile->num_dims(); dimidx<limit; ++dimidx) {
      NcDim *dim = gridfile->get_dim(dimidx);
      in_dims[dim->name()] = dim;
    }
  }

  // Gather variables from the input and grid files.
  // The variables from the grid file are read last and overwrite anything
  // found in the input file.
  for (int varidx=0, limit=infile->num_vars(); varidx<limit; ++varidx) {
    NcVar *var = infile->get_var(varidx);
    in_vars[var->name()] = var;
    in_var_parents[var->name()] = infile->id();
  }
  if (infile != gridfile) { // small optimization
    for (int varidx=0, limit=gridfile->num_vars(); varidx<limit; ++varidx) {
      NcVar *var = gridfile->get_var(varidx);
      in_vars[var->name()] = var;
      in_var_parents[var->name()] = gridfile->id();
    }
  }

  // Iterate over our gathered dimensions and create masks based on them.
  create_masks(masks, in_dims);
  // Adjust the masks based on any command-line parameters.
  adjust_masks(masks, in_dims, slices);
  if (wants_region) {
    // Adjust the lat/lon (cells) mask based on the "box" cmd line param.
    // This also appropriately adjust the masks for corners and edges.
    // HACK - We know our stuff is in radians
    box.scale(DEG2RAD);
    adjust_cells_mask(masks, in_dims, box, gridfile);
  }

  // Now that our masks are created, we can generate the remapping vectors.
  if (masks.find("cells") != masks.end()) {
    bool *mask = masks["cells"];
    cells_map = new int[in_dims["cells"]->size()];
    for (int i=0,limit=in_dims["cells"]->size(),remap=-1; i<limit; ++i) {
      if (mask[i]) {
        cells_map[i] = ++remap;
      } else {
        cells_map[i] = remap;
      }
    }
  }
  if (masks.find("corners") != masks.end()) {
    bool *mask = masks["corners"];
    corners_map = new int[in_dims["corners"]->size()];
    for (int i=0,limit=in_dims["corners"]->size(),remap=-1; i<limit; ++i) {
      if (mask[i]) {
        corners_map[i] = ++remap;
      } else {
        corners_map[i] = remap;
      }
    }
  }
  if (masks.find("edges") != masks.end()) {
    bool *mask = masks["edges"];
    edges_map = new int[in_dims["edges"]->size()];
    for (int i=0,limit=in_dims["edges"]->size(),remap=-1; i<limit; ++i) {
      if (mask[i]) {
        edges_map[i] = ++remap;
      } else {
        edges_map[i] = remap;
      }
    }
  }

  // Now that our masks are created, we can tally up their new sizes.
  for (MaskMap::iterator it=masks.begin(); it!=masks.end(); ++it) {
    string name = it->first;
    bool *mask = it->second;
    long size = in_dims[name]->size();
    long new_size = count(mask, mask+size, true);
    out_dim_sizes[name] = new_size;
    //cout << name << " " << size << " " << new_size << endl;
  }

  // Now that we have absolutely every mask that we're going to need,
  // we must now define our output netcdf file, its dimensions, and its
  // variables along with associated metadata.
  copy_global_attributes(outfile, infile, gridfile);
  out_dims = add_dims(outfile, out_dim_sizes);
  out_vars = add_vars(outfile, out_dims, in_vars, in_var_parents);
  
  // By now we have the output file completely defined.
  // All that's left to do is copy the data.
  // A recursive function to read a subsetted variable entirely into
  // memory (a single-segment buffer). Then write this buffer to outfile.
  // Iterate over variables in our VarMap and call recursive funciton with each.
  for (VarMap::iterator it=out_vars.begin(); it!=out_vars.end(); ++it) {
    string name = it->first;
    NcVar *out_var = it->second;
    NcVar *in_var = in_vars[name];
    int num_dims = in_var->num_dims();
    bool **var_masks = new bool*[num_dims];
    long *packed_sizes = new long[num_dims];
    for (int dimidx=0; dimidx<num_dims; ++dimidx) {
      string dim_name = in_var->get_dim(dimidx)->name();
      var_masks[dimidx] = masks[dim_name];
      packed_sizes[dimidx] = out_dim_sizes[dim_name];
    }
    int *remap = NULL;
    if (name == "cell_neighbors") {
      remap = cells_map;
    } else if (name == "cell_corners") {
      remap = corners_map;
    } else if (name == "cell_edges") {
      remap = edges_map;
    }
    copy_variable(in_var, out_var, var_masks, packed_sizes, remap);
    delete [] var_masks;
    delete [] packed_sizes;
  }

  if (infile != gridfile) delete gridfile;
  delete infile;
  delete outfile;

  return EXIT_SUCCESS;
}


template <class T>
T* read_var(NcVar *var)
{
  int num_dims = var->num_dims();
  long *edges = var->edges();
  long *current = new long[num_dims];
  long size = 1;
  for (int dim_index=0; dim_index<num_dims; ++dim_index) {
    size *= edges[dim_index];
    current[dim_index] = 0;
  }
  T *data = new T[size];
  var->set_cur(current);
  if (!var->get(&data[0], edges)) {
    cout << "ERROR: Could not read data for var '" << var->name() << "'" << endl;
    exit(EXIT_FAILURE);
  }
  delete [] edges;
  delete [] current;
  return data;
}


/**
 * Reads the entire variable var into memory based on the supplied masks.
 */
template <class T>
T* read_var(NcVar *var, bool **masks, long *packed_sizes)
{
  int num_dims = var->num_dims();
  long size = 1;
  for (int dimidx=0; dimidx<num_dims; ++dimidx) {
    size *= packed_sizes[dimidx];
  }
  T *out = new T[size];

  T *in = read_var<T>(var);

  long *edges = var->edges();
  long out_idx = 0;
  long in_idx = 0;
  // strides are a tiny bit tricky to calculate, more bothersome, really
  long *strides = new long[num_dims];
  strides[num_dims-1] = 1;
  for (int i=num_dims-2; i>=0; --i) {
    strides[i] = strides[i+1]*edges[i+1];
  }
  // optimization -- if this var's dimensions were not masked, just return it
  int i;
  for (i=0; i<num_dims; ++i) {
    if (edges[i] != packed_sizes[i]) {
      _read_var_recursive(out, out_idx, in, in_idx, masks, edges, strides,
                          num_dims, 0);
      break;
    }
  }
  delete [] edges;
  delete [] strides;
  if (i == num_dims) {
    delete [] out;
    return in;
  } else {
    delete [] in;
    return out;
  }
}


template <class T>
void _read_var_recursive(T *out, long& out_idx, T *in, long &in_idx,
        bool **masks, long *edges, long *strides, int num_dims, int depth)
{
  if (depth == num_dims-1) {
    for (int i=0, limit=edges[depth]; i<limit; ++i) {
      if (masks[depth][i]) {
        out[out_idx++] = in[in_idx++];
      } else {
        ++in_idx;
      }
    }
  } else {
    for (int i=0, limit=edges[depth]; i<limit; ++i) {
      if (masks[depth][i]) {
        _read_var_recursive(out, out_idx, in, in_idx,
                masks, edges, strides, num_dims, depth+1);
      } else {
        in_idx += strides[depth];
      }
    }
  }
}


void create_masks(MaskMap &masks, DimMap &dims)
{
  for (DimMap::iterator it=dims.begin(); it!=dims.end(); ++it) {
    string name = it->first;
    NcDim *dim = it->second;
    long size = dim->size();
    bool *mask = new bool[size];
    fill(mask, mask+size, true);
    masks[name] = mask;
  }
}


void adjust_masks(MaskMap &masks, DimMap &dims, DimSlices slices)
{
  // By default, all masks are filled with "true".
  // If a slice was specified on the command line, we must reset that fill
  // value to "false".  This variable keeps track of which masks we 
  // re-filled with "false" so that it's only done once.
  // Recall that a user could specify "hyperslabs".
  set<string> already_filled;

  for (DimSlices::iterator it=slices.begin(); it!=slices.end(); ++it) {
    DimensionSlice slice = *it;
    string name = slice.name;
    // first, make sure slice corresponds to an actual dimension
    MaskMap::iterator mask_iter = masks.find(name);
    if (mask_iter == masks.end()) {
      cout << "Sliced dimension '" << name << "' does not exist" << endl;
      exit(EXIT_FAILURE);
    }
    long size = dims[name]->size();
    bool *mask = mask_iter->second;
    // we must fill the dimension mask with false now
    if (already_filled.count(name) == 0) {
      already_filled.insert(name);
      fill(mask, mask+size, false);
    }
    long start,stop,step;
    slice.indices(size, start, stop, step);
    if (step < 0) {
      for (int maskidx=start; maskidx>stop; maskidx+=step) {
        mask[maskidx] = true;
      }
    } else {
      for (int maskidx=start; maskidx<stop; maskidx+=step) {
        mask[maskidx] = true;
      }
    }
  }
}


void adjust_cells_mask(MaskMap &masks, DimMap &dims, LatLonBox box,
                       NcFile *gridfile)
{
  // First we need the grid_center_lon/lat variables from the gridfile.
  NcVar *var_grid_center_lon = gridfile->get_var("grid_center_lon");
  NcVar *var_grid_center_lat = gridfile->get_var("grid_center_lat");
  if (!var_grid_center_lon->is_valid()) {
    cout << "Couldn't locate variable grid_center_lon" << endl;
    exit(EXIT_FAILURE);
  }
  if (!var_grid_center_lat->is_valid()) {
    cout << "Couldn't locate variable grid_center_lat" << endl;
    exit(EXIT_FAILURE);
  }
  float *lat = read_var<float>(var_grid_center_lat);
  float *lon = read_var<float>(var_grid_center_lon);
  bool *cells_mask = masks["cells"];

  // We also need the cell_corners and cell_edges, if they exist
  NcVar *var_cell_corners = gridfile->get_var("cell_corners");
  NcVar *var_cell_edges = gridfile->get_var("cell_edges");
  int *cell_corners = NULL;
  int *cell_edges = NULL;
  bool *corners_mask = NULL;
  bool *edges_mask = NULL;
  if (var_cell_corners) {
    cell_corners = read_var<int>(var_cell_corners);
    corners_mask = masks["corners"];
  }
  if (var_cell_edges) {
    cell_edges = read_var<int>(var_cell_edges);
    edges_mask = masks["edges"];
  }

  int num_cells = dims["cells"]->size();
  int corners_per_cell = dims["cellcorners"]->size();
  int edges_per_cell = dims["celledges"]->size();

  for (long cellidx=0; cellidx<num_cells; ++cellidx) {
    cells_mask[cellidx] = box.contains(lat[cellidx],lon[cellidx]);
    if (var_cell_corners) {
      for (int corneridx=0; corneridx<corners_per_cell; ++corneridx) {
        int cell_corners_idx = cellidx*corners_per_cell + corneridx;
        corners_mask[cell_corners[cell_corners_idx]] = cells_mask[cellidx];
      }
    }
    if (var_cell_edges) {
      for (int edgeidx=0; edgeidx<edges_per_cell; ++edgeidx) {
        int cell_edges_idx = cellidx*edges_per_cell + edgeidx;
        edges_mask[cell_edges[cell_edges_idx]] = cells_mask[cellidx];
      }
    }
  }
  delete [] lat;
  delete [] lon;
}


/**
 * Copy all global attributes from infile and gridfile to outfile.
 *
 * Special care is only taken with the "history" attribute which will combine
 * the attributes from each file.
 */
void copy_global_attributes(NcFile *outfile, NcFile *infile, NcFile *gridfile)
{
  for (int attidx=0, limit=infile->num_atts(); attidx<limit; ++attidx) {
    NcAtt *att = infile->get_att(attidx);
    int status = nc_copy_att(infile->id(), NC_GLOBAL, att->name(),
                             outfile->id(), NC_GLOBAL);
    if (status != NC_NOERR) {
      cout << "ERROR: Could not copy global attribute '" << att->name()
           << "'" << endl;
      exit(EXIT_FAILURE);
    }
    delete att;
  }
}


DimMap add_dims(NcFile *outfile, DimSizeMap sizes)
{
  DimMap dims;

  for (DimSizeMap::iterator it=sizes.begin(); it!=sizes.end(); ++it) {
    string name = it->first;
    long size = it->second;
    NcDim *dim;
    if (name == "time") {
      dim = outfile->add_dim("time"); // unlimited dim
      if (!dim) {
        cout << "ERROR: Could not create unlimited dimension 'time'" << endl;
        exit(EXIT_FAILURE);
      }
    } else {
      dim = outfile->add_dim(name.c_str(), size);
      if (!dim) {
        cout << "ERROR: Could not create dimension '" << name << "'" << endl;
        exit(EXIT_FAILURE);
      }
    }
    dims[name] = dim;
  }

  return dims;
}


VarMap add_vars(NcFile *outfile, DimMap out_dims, VarMap in_vars,
                VarParentIdMap in_var_parents)
{
  VarMap out_vars;

  for (VarMap::iterator it=in_vars.begin(); it!=in_vars.end(); ++it) {
    string var_name = it->first;
    NcVar *in_var = it->second;
    int num_dims = in_var->num_dims();
    NcDim **dims = new NcDim*[num_dims];
    for (int dimidx=0; dimidx<num_dims; ++dimidx) {
      dims[dimidx] = out_dims[in_var->get_dim(dimidx)->name()];
    }
    NcVar *out_var = outfile->add_var(var_name.c_str(), in_var->type(),
                                      num_dims, (const NcDim**)dims);
    out_vars[var_name] = out_var;
    delete [] dims;

    // now the attributes
    for (int attidx=0, limit=in_var->num_atts(); attidx<limit; ++attidx) {
      NcAtt *att = in_var->get_att(attidx);
      int status = nc_copy_att(in_var_parents[var_name], in_var->id(),
                               att->name(), outfile->id(), out_var->id());
      if (status != NC_NOERR) {
        cout << "ERROR: Could not copy global attribute '" << att->name()
            << "'" << endl;
        exit(EXIT_FAILURE);
      }
      delete att;
    }
  }

  return out_vars;
}


void copy_variable(NcVar *in, NcVar *out, bool **masks, long *packed_sizes,
                   int *remap)
{
  // bail if any dim was completely cut out
  // this also catches record variables for a zero-length record file
  for (int i=0; i<in->num_dims(); ++i) {
    if (packed_sizes[i] == 0) return;
  }
  switch (in->type()) {
    case ncInt:
      _copy_variable_int(in, out, masks, packed_sizes, remap);
      break;
    case ncFloat:
      _copy_variable<float>(in, out, masks, packed_sizes);
      break;
    case ncDouble:
      _copy_variable<double>(in, out, masks, packed_sizes);
      break;
    default:
      cout << "Defaulting for type " << in->type() << endl;
      break;
  }
}


void _copy_variable_int(NcVar *in, NcVar *out, bool **masks,
                        long *packed_sizes, int *remap)
{
  int *data = read_var<int>(in, masks, packed_sizes);
  if (remap != NULL) {
    long limit = 1;
    for (long dimidx=0,num_dims=in->num_dims(); dimidx<num_dims; ++dimidx) {
      limit *= packed_sizes[dimidx];
    }
    for (long i=0; i<limit; ++i) {
      data[i] = remap[data[i]];
    }
  }
  out->put(data, packed_sizes);
  delete [] data;
}

template <class T>
void _copy_variable(NcVar *in, NcVar *out, bool **masks, long *packed_sizes)
{
  T *data = read_var<T>(in, masks, packed_sizes);
  out->put(data, packed_sizes);
  delete [] data;
}

