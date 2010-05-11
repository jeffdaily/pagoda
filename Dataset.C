#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <cctype>
#include <iostream>
#include <set>

#include "Attribute.H"
#include "BoundaryVariable.H"
#include "ConnectivityVariable.H"
#include "CoordinateVariable.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "DistributedMask.H"
#include "Error.H"
#include "LatLonBox.H"
#include "LocalMask.H"
#include "Mask.H"
#include "NetcdfDataset.H"
#include "NetcdfVariable.H"
#include "PnetcdfTiming.H"
#include "Slice.H"
#include "StringComparator.H"
#include "Timing.H"
#include "Util.H"
#include "Variable.H"

using std::cerr;
using std::endl;
using std::fill;
using std::find;
using std::set;
using std::tolower;
using std::transform;


Dataset* Dataset::open(const string &filename)
{
    //TIMING("Dataset::open(string)");
    Dataset *dataset = NULL;
    string EXT_NC(".nc");
    if (Util::ends_with(filename, EXT_NC)) {
        dataset = new NetcdfDataset(filename);
    }
    if (dataset) {
        //dataset->decorate();
        //dataset->create_masks();
    }
    return dataset;
}


Dataset::Dataset()
{
    TIMING("Dataset::Dataset()");
}


Dataset::~Dataset()
{
    TIMING("Dataset::~Dataset()");
}


Attribute* Dataset::find_att(const string &name, bool ignore_case, bool within)
{
    TIMING("Dataset::find_att(string,bool,bool)");
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::const_iterator it = atts.begin();
    vector<Attribute*>::const_iterator end = atts.end();
    StringComparator cmp;

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(name)) {
            return *it;
        }
    }

    return NULL;
}


Attribute* Dataset::find_att(const vector<string> &names, bool ignore_case,
        bool within)
{
    TIMING("Dataset::find_att(vector<string>,bool,bool)");
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::const_iterator it = atts.begin();
    vector<Attribute*>::const_iterator end = atts.end();
    StringComparator cmp;

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(names)) {
            return *it;
        }
    }

    return NULL;
}


Dimension* Dataset::find_dim(const string &name, bool ignore_case, bool within)
{
    TIMING("Dataset::find_dim(string,bool,bool)");
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::const_iterator it = dims.begin();
    vector<Dimension*>::const_iterator end = dims.end();
    StringComparator cmp;

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(name)) {
            return *it;
        }
    }

    return NULL;
}


Variable* Dataset::find_var(const string &name, bool ignore_case, bool within)
{
    TIMING("Dataset::find_var(string,bool,bool)");
    vector<Variable*> vars = get_vars();
    vector<Variable*>::const_iterator it = vars.begin();
    vector<Variable*>::const_iterator end = vars.end();
    StringComparator cmp;

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(name)) {
            return *it;
        }
    }

    return NULL;
}


void Dataset::create_masks()
{
    TIMING("Dataset::create_masks()");
    vector<Mask*> masks;
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::iterator dim_it;
    for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
        Dimension *dim = *dim_it;
        masks.push_back(new DistributedMask(dim, 1));
    }
    populate_masks(masks);
}


void Dataset::populate_masks(const vector<Mask*> &masks)
{
    TIMING("Dataset::populate_masks(vector<Mask*>)");
    vector<Mask*>::const_iterator masks_it = masks.begin();
    vector<Mask*>::const_iterator masks_end = masks.end();
    for (; masks_it!=masks_end; ++masks_it) {
        Mask *mask = *masks_it;
        Dimension *dim = find_dim(mask->get_name());
        if (dim) {
            dim->set_mask(mask);
        }
    }
}


void Dataset::adjust_masks(const vector<DimSlice> &slices)
{
    TIMING("Dataset::adjust_masks(vector<DimSlice)");
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::iterator dim_it;
    vector<DimSlice>::const_iterator slice_it;

    // we're iterating over the command-line specified slices to create masks
    for (slice_it=slices.begin(); slice_it!=slices.end(); ++slice_it) {
        DimSlice slice = *slice_it;
        //Mask *mask;

        // find the Dimension
        for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
            Dimension *dim = *dim_it;
            string dim_name = dim->get_name();
            string slice_name = slice.get_name();
            if (dim_name == slice_name) {
                break;
            }
        }
        if (dim_it == dims.end()) {
            const string name = slice.get_name();
            PRINT_ZERO("Sliced dimension '%s' does not exist\n", name.c_str());
            continue;
        } else {
            Dimension *dim = *dim_it;
            // adjust the Mask based on the current Slice
            dim->get_mask()->adjust(slice);
        }
    }
}


//#define ADJUST_MASKS_WITHOUT_PRESERVATION 1
#if ADJUST_MASKS_WITHOUT_PRESERVATION
/**
 * Special-case mask creation using a Lat/Lon combination.
 *
 * Different than creating masks solely based on a single Slice at a time,
 * this special case allows the lat and lon dimensions to be subset
 * simultaenously. This is most important when dealing with a cell-based
 * grid where the lat and lon coordinate variables share the same dimension.
 * In that case, if we were to generate masks one at a time, we'd get, in
 * logic terms, an OR over the shared dimension mask. What we really want,
 * whether the grid is cell- or rectangular-based, is an AND result -- a
 * lat lon box instead of a lat lon cross.
 *
 * This algorithm does not preserve whole cells, rather the unique cells,
 * corners, and edges are all subset individually.
 */
void Dataset::adjust_masks(const LatLonBox &box)
{
    TIMING("Dataset::adjust_masks(LatLonBox)");
    if (box != LatLonBox::GLOBAL) {
        // TODO how to locate the lat/lon dims?
        // TODO subset anything with lat or lon dimensions such as
        // corner/edge variables
        // Likely solution is to iterate over all Variables and examine them
        // for special attributes
        Variable *lat;
        Variable *lon;

        lat = find_var(string("grid_center_lat"), false, true);
        lon = find_var(string("grid_center_lon"), false, true);
        if (!lat) {
            PRINT_ZERO("adjust_masks: missing grid_center_lat\n");
        } else if (!lon) {
            PRINT_ZERO("adjust_masks: missing grid_center_lon\n");
        } else {
            lon->get_dims()[0]->get_mask()->adjust(box, lat, lon);
        }
        lat = find_var(string("grid_corner_lat2"), false, true);
        lon = find_var(string("grid_corner_lon2"), false, true);
        if (!lat) {
            PRINT_ZERO("adjust_masks: missing grid_corner_lat\n");
        } else if (!lon) {
            PRINT_ZERO("adjust_masks: missing grid_corner_lon\n");
        } else {
            lon->get_dims()[0]->get_mask()->adjust(box, lat, lon);
        }
        lat = find_var(string("grid_edge_lat2"), false, true);
        lon = find_var(string("grid_edge_lon2"), false, true);
        if (!lat) {
            PRINT_ZERO("adjust_masks: missing grid_corner_lat\n");
        } else if (!lon) {
            PRINT_ZERO("adjust_masks: missing grid_corner_lon\n");
        } else {
            lon->get_dims()[0]->get_mask()->adjust(box, lat, lon);
        }
    }
}

#else /* ADJUST_MASKS_WITHOUT_PRESERVATION */

static inline void adjust_corners_edges_mask(
        Dataset *dataset, int *cmask, int64_t clo, int64_t chi, bool corners)
{
    Variable *var;
    ConnectivityVariable *cv;
    NetcdfVariable *ncv;
    Dimension *dim;
    Dimension *dimbound;
    DistributedMask *mask;
    string var_name = corners ? "cell_corners" : "cell_edges";
    string dim_name = corners ? "corners" : "edges";
    int ZERO = 0;
    int64_t cells_count = chi-clo+1;
    
    var = dataset->find_var(var_name, false, true);
    dim = dataset->find_dim(dim_name);

    if (!var) {
        if (corners)
            PRINT_ZERO("no cell_corners found to subset\n");
        else
            PRINT_ZERO("no cell_edges found to subset\n");
        return;
    }
    if (!dim) {
        if (corners) {
            PRINT_ZERO("missing associated dimension 'corners' for cell_corner\n");
            PRINT_ZERO("cannot subset cell_corner\n");
        } else {
            PRINT_ZERO("missing associated dimension 'edges' for cell_edges\n");
            PRINT_ZERO("cannot subset cell_edge\n");
        }
        return;
    }
    mask = dynamic_cast<DistributedMask*>(dim->get_mask());
    cv = dynamic_cast<ConnectivityVariable*>(var);
    ncv = dynamic_cast<NetcdfVariable*>(var);
    if (!ncv) {
        if (!cv) {
            ERR("did not cast to ConnectivityVariable");
        } else {
            ncv = dynamic_cast<NetcdfVariable*>(cv->get_var());
            if (!ncv) {
                ERR("did not cast to NetcdfVariable");
            }
        }
    }
    dimbound = var->get_dims().at(1);

    GA_Fill(mask->get_handle(), &ZERO);
    
    // read portion of the corners or edges local to the cells mask
    int64_t per_cell = dimbound->get_size();
    int64_t local_size = cells_count * per_cell;
    MPI_Offset var_lo[] = {clo, 0};
    MPI_Offset var_count[] = {cells_count, per_cell};
    int var_data[local_size];
    ncmpi::get_vara_all(ncv->get_dataset()->get_id(), ncv->get_id(),
                var_lo, var_count, &var_data[0]);

    // iterate over the corners or edges indices and place into set
    set<int> var_data_set;
    for (int64_t cellidx=0; cellidx<cells_count; ++cellidx) {
        if (cmask[cellidx] != 0) {
            int64_t local_offset = cellidx * per_cell;
            for (int64_t boundidx=0; boundidx<per_cell; ++boundidx) {
                var_data_set.insert(var_data[local_offset+boundidx]);
            }
        }
    }

    // NGA_Scatter_acc expects int** (yuck)
    size_t size = var_data_set.size();
    int ones[size];
    fill(ones, ones+size, 1);
    int64_t *subs[size];
    set<int>::const_iterator it,end;
    size_t idx=0;
    for (it=var_data_set.begin(),end=var_data_set.end(); it!=end; ++it,++idx) {
        subs[idx] = new int64_t(*it);
    }

    NGA_Scatter64(mask->get_handle(), ones, subs, size);
    
    // Clean up!
    for (int64_t idx=0,limit=size; idx<limit; ++idx) {
        delete subs[idx];
    }
}


static inline bool adjust_centers_mask(Dataset *dataset, const LatLonBox &box)
{
    TRACER("adjust_centers_mask\n");
    int type;
    int ndim;
    int64_t dims[1];
    int64_t lo;
    int64_t hi;
    DistributedMask *cells_mask;
    int *cells_mask_data;
    Variable *lat_var;
    Variable *lon_var;
    Dimension *cells_dim;

    lat_var = dataset->find_var(string("grid_center_lat"), false, true);
    lon_var = dataset->find_var(string("grid_center_lon"), false, true);
    if (!lat_var) {
        ERR("adjust_masks: missing grid_center_lat");
    }
    if (!lon_var) {
        ERR("adjust_masks: missing grid_center_lon");
    }

    cells_dim = lat_var->get_dims()[0];
    cells_mask = dynamic_cast<DistributedMask*>(cells_dim->get_mask());
    if (!cells_mask) {
        ERR("cells mask was not distributed");
    }

    lat_var->read();
    lon_var->read();
    if (!cells_mask->access(lo, hi, cells_mask_data)) {
        DEBUG_SYNC("this process doesn't own any of the cells_mask, return\n");
        return false;
    } else {
        DEBUG_SYNC("this process owns some of the cells_mask, continue\n");
    }
    // just need the type of the lat/lon vars
    NGA_Inquire64(lat_var->get_handle(), &type, &ndim, dims);
    switch (type) {
#define adjust_op(MTYPE,TYPE) \
        case MTYPE: { \
            TYPE *lat; \
            TYPE *lon; \
            NGA_Access64(lat_var->get_handle(), &lo, &hi, &lat, NULL); \
            NGA_Access64(lon_var->get_handle(), &lo, &hi, &lon, NULL); \
            for (int64_t i=0, limit=hi-lo+1; i<limit; ++i) { \
                cells_mask_data[i] = box.contains(lat[i], lon[i]) ? 1 : 0; \
            } \
            break; \
        }
        adjust_op(C_INT,int)
        adjust_op(C_LONG,long)
        adjust_op(C_LONGLONG,long long)
        adjust_op(C_FLOAT,float)
        adjust_op(C_DBL,double)
#undef adjust_op
    }
    adjust_corners_edges_mask(dataset, cells_mask_data, lo, hi, true);
    adjust_corners_edges_mask(dataset, cells_mask_data, lo, hi, false);
    cells_mask->release();

    return true;
}


/**
 * Special-case mask creation using a Lat/Lon combination.
 *
 * Different than creating masks solely based on a single Slice at a time,
 * this special case allows the lat and lon dimensions to be subset
 * simultaenously. This is most important when dealing with a cell-based
 * grid where the lat and lon coordinate variables share the same dimension.
 * In that case, if we were to generate masks one at a time, we'd get, in
 * logic terms, an OR over the shared dimension mask. What we really want,
 * whether the grid is cell- or rectangular-based, is an AND result -- a
 * lat lon box instead of a lat lon cross.
 *
 * This algorithm preserves whole cells.
 */
void Dataset::adjust_masks(const LatLonBox &box)
{
    TRACER("Dataset::adjust_masks(LatLonBox)\n");
    TIMING("Dataset::adjust_masks(LatLonBox)");
    // TODO THIS CODE SHOULD BE MOVED
    // The distributed nature of the data should be hidden from the 
    // Dataset class IMHO. This code also assumes the all of the variables
    // here are distributed (which is a generally safe assumption.)
    if (box == LatLonBox::GLOBAL) {
        return;
    }

    if (!adjust_centers_mask(this, box)) {
        // this process didn't own any of the data
        return;
    }
}


#endif /* ADJUST_MASKS_WITHOUT_PRESERVATION */



ostream& operator << (ostream &os, const Dataset *dataset)
{
    return dataset->print(os);
}


/**
 * Examine attributes and enhance/wrap Variables appropriately.
 */
void Dataset::decorate()
{
    TIMING("Dataset::decorate()");
    static vector<string> lat_units;
    if (lat_units.empty()) {
        lat_units.push_back(string("degrees_north"));
        lat_units.push_back(string("degree_north"));
        lat_units.push_back(string("degree_N"));
        lat_units.push_back(string("degrees_N"));
        lat_units.push_back(string("degreeN"));
        lat_units.push_back(string("degreesN"));
    }
    static vector<string> lon_units;
    if (lon_units.empty()) {
        lon_units.push_back(string("degrees_east"));
        lon_units.push_back(string("degree_east"));
        lon_units.push_back(string("degree_E"));
        lon_units.push_back(string("degrees_E"));
        lon_units.push_back(string("degreeE"));
        lon_units.push_back(string("degreesE"));
    }
    static vector<string> vert_units;
    if (vert_units.empty()) {
        vert_units.push_back(string("bar"));
        vert_units.push_back(string("millibar"));
        vert_units.push_back(string("decibar"));
        vert_units.push_back(string("atm")); // atmosphere
        vert_units.push_back(string("Pa")); // pascal
        vert_units.push_back(string("hPa")); // hectopascal
        vert_units.push_back(string("meter"));
        vert_units.push_back(string("metre"));
        vert_units.push_back(string("m")); // meter
        vert_units.push_back(string("kilometer"));
        vert_units.push_back(string("km")); // kilometer
        // deprecated
        vert_units.push_back(string("level"));
        vert_units.push_back(string("layer"));
        vert_units.push_back(string("interface"));
        vert_units.push_back(string("sigma_level"));
    }

    vector<Variable*> vars = get_vars();
    vector<Variable*>::iterator var_it;

    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        //cerr << "Attempting to decorate " << var->get_name() << endl;
        Attribute *att;
        if ((att = var->find_att(string("bounds")))) {
            string val = att->get_string();
            Variable *bound_var;
            if ((bound_var = find_var(val))) {
                vector<Variable*>::iterator it;
                it = find(vars.begin(), vars.end(), bound_var);
                (*it) = new BoundaryVariable(bound_var, var);
            }
        }
        if ((att = var->find_att(string("units")))) {
            string val = att->get_string();
            if (find(lat_units.begin(),lat_units.end(),val) != lat_units.end())
            {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
            if (find(lon_units.begin(),lon_units.end(),val) != lon_units.end())
            {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
            if (find(vert_units.begin(),vert_units.end(),val)
                    != vert_units.end()) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
        }
        if ((att = var->find_att(string("standard_name")))) {
            string val = att->get_string();
            if ("latitude" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            } else if ("longitude" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
        }
        if ((att = var->find_att(string("axis")))) {
            string val = att->get_string();
            if ("Y" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            } else if ("X" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            } else if ("Z" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
        }
        if ((att = var->find_att(string("positive")))) {
            string val = att->get_string();
            transform(val.begin(),val.end(),val.begin(),(int(*)(int))tolower);
            if ("up" == val || "down" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
        }
        if (var->get_name() == "cell_neighbors") {
            Dimension *dim = find_dim(string("cells"));
            if (dim) {
                (*var_it) = new ConnectivityVariable(var, dim);
            }
        }
        if (var->get_name() == "cell_corners") {
            Dimension *dim = find_dim(string("corners"));
            if (dim) {
                (*var_it) = new ConnectivityVariable(var, dim);
            }
        }
        if (var->get_name() == "cell_edges") {
            Dimension *dim = find_dim(string("edges"));
            if (dim) {
                (*var_it) = new ConnectivityVariable(var, dim);
            }
        }
    }
#ifdef DEBUG
    if (0 == GA_Nodeid()) {
        cerr << "Variables after decoration" << endl;
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            cerr << (*var_it) << endl;
        }
    }
    GA_Sync();
#endif // DEBUG

    decorate_set(vars);
}
