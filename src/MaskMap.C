#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>
#include <map>
#include <string>
#include <vector>

using std::ostream;
using std::map;
using std::string;
using std::vector;

#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "LatLonBox.H"
#include "Mask.H"
#include "MaskMap.H"
#include "NotImplementedException.H"
#include "Slice.H"
#include "Timing.H"

MaskMap::MaskMap()
    :   masks()
{
}


MaskMap::~MaskMap()
{
}


/**
 * Create a default Mask for each Dimension in the given Dataset.
 *
 * If a Mask does not yet exist for a Dimension, it is initialized to 1.
 * Otherwise, if a Mask already exists, it is left alone.
 */
void MaskMap::create_masks(const Dataset *dataset)
{
    vector<Dimension*> dims = dataset->get_dims();
    vector<Dimension*>::iterator dim_it;

    TIMING("MaskMap::create_masks(Dataset*)");

    for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
        Dimension *dim = *dim_it;
        Mask *mask = get_mask(dim);
    }
}


void MaskMap::modify_masks(
        const vector<DimSlice> &slices,
        const vector<Dimension*> &dims)
{
    TIMING("Dataset::modify_masks(vector<DimSlice>,vector<Dimension*>)");
    vector<Dimension*>::const_iterator dim_it;
    vector<DimSlice>::const_iterator slice_it;

    // we're iterating over the command-line specified slices to create masks
    for (slice_it=slices.begin(); slice_it!=slices.end(); ++slice_it) {
        DimSlice slice = *slice_it;
        string slice_name = slice.get_name();

        // find the Dimension
        for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
            Dimension *dim = *dim_it;
            string dim_name = dim->get_name();
            if (dim_name == slice_name) {
                // modify the Mask based on the current Slice
                Mask *mask = get_mask(dim);
                mask->modify(slice);
                break;
            }
        }
        if (dim_it == dims.end()) {
            const string name = slice.get_name();
            PRINT_ZERO("Sliced dimension '%s' does not exist\n", name.c_str());
        }
    }
}


/**
 * Modify the shared mask of the given lat/lon variables.
 *
 * For instance the lat/lon variables for corners, edges, and cells would have
 * their masks modified based on each pair of variables.
 *
 * @param[in] box the lat/lon specification
 * @param[in] lat the latitude coordinate data
 * @param[in] lon the longitude coordinate data
 * @param[in] dim the dimension we are masking
 */
void MaskMap::modify_masks(const LatLonBox &box,
        const Array *lat, const Array *lon, Dimension *dim)
{
    Mask *mask = get_mask(dim);
    mask->modify(box, lat, lon);
}


/**
 * Modifiy the masks of the given lat/lon variables.
 *
 * This is usefulf when the lat/lon dimensions are unique.  In other words,
 * assumes lat_dim and lon_dim are two different dimensions.
 *
 * @param[in] box the lat/lon specification
 * @param[in] lat the latitude coordinate data
 * @param[in] lon the longitude coordinate data
 * @param[in] lat_dim the latitude dimension we are masking
 * @param[in] lon_dim the longitude dimension we are masking
 */
void MaskMap::modify_masks(
        const LatLonBox &box, const Array *lat, const Array *lon,
        Dimension *lat_dim, Dimension *lon_dim)
{
    throw NotImplementedException("MaskMap::modify_masks(LatLonBox,Array*,Array*,Dimension*,Dimension*)");
}


/**
 * Modify the mask of a connected Dimension.
 *
 * This is a very specific function designed to help preserve whole cells
 * during a subset.  Let's assume we have cells/faces, edges, and
 * vertices/corners within a dataset and we also have variables indicating
 * how they are connected.  For a variable from cells to edges might look like
 * cell_edges(cells, celledges) where celledges indicates how many edges the
 * particular cell has.  We want to mask the related edges dimension (don't
 * confuse with the small celledges Dimension).  That's where this function
 * is useful.  Read it as: modify the mask for the "to_mask" Dimension based
 * on the given "masked" Dimension and the given "topology" relation.
 *
 * @param[in] masked the Dimension which already has an associated Mask
 * @param[in] to_mask the Dimension which should be masked based on masked
 * @param[in] topology relation between the two given Dimensions
 */
void MaskMap::modify_masks(
        Dimension *dim_masked, Dimension *dim_to_mask, const Array *topology)
{
    Mask *mask = get_mask(dim_masked);
    Mask *to_mask = get_mask(dim_to_mask);
    int64_t connections = topology->get_shape().at(1);
    int64_t mask_local_size = mask->get_local_size();
    vector<int64_t> masked_lo(1);
    vector<int64_t> masked_hi(1);
    vector<int64_t> topology_lo(2);
    vector<int64_t> topology_hi(2);
    int *mask_data;
    int *topology_data;
    vector<int> topology_buffer;
    vector<int64_t> topology_subscripts;

    if (!mask->owns_data()) {
        // bail if this process doesn't own any of the mask
        return;
    }
    
    mask->get_distribution(masked_lo, masked_hi);
    topology_lo.push_back(masked_lo.at(0));
    topology_lo.push_back(0);
    topology_hi.push_back(mask_local_size);
    topology_hi.push_back(connections);
    
    // read portion of the topology local to the mask
    topology_data = (int*)topology->get(topology_lo, topology_hi);
    
    // iterate over the topology indices and place into set
    mask_data = (int*)mask->access();
    for (int64_t idx=0; idx<mask_local_size; ++idx) {
        if (mask_data[idx] != 0) {
            int64_t local_offset = idx * connections;
            for (int64_t connection=0; connection<connections; ++connection) {
                topology_subscripts.push_back(
                        topology_data[local_offset+connection]);
            }
        }
    }
    mask->release();

    topology_buffer.assign(topology_subscripts.size(), 1);
    to_mask->scatter(&topology_buffer[0], topology_subscripts);

    // clean up
    delete [] topology_data;
}


Mask* MaskMap::get_mask(Dimension *dim)
{
    return get_mask(dim->get_name(), dim);
}


Mask* MaskMap::get_mask(const string &name, const Dimension *dim)
{
    masks_t::iterator it = masks.find(name);
    if (it == masks.end()) {
        return masks.insert(make_pair(name, Mask::create(dim))).first->second;
    } else {
        return it->second;
    }
}


Mask* MaskMap::operator [] (Dimension *dim)
{
    return get_mask(dim);
}


ostream& MaskMap::print(ostream &os) const
{
    os << "MaskMap " << this;
    return os;
}


ostream& operator << (ostream &os, const MaskMap &maskmap)
{
    maskmap.print(os);
    return os;
}


ostream& operator << (ostream &os, const MaskMap *maskmap)
{
    maskmap->print(os);
    return os;
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

        lat = get_var(string("grid_center_lat"), false, true);
        lon = get_var(string("grid_center_lon"), false, true);
        if (!lat) {
            PRINT_ZERO("adjust_masks: missing grid_center_lat\n");
        } else if (!lon) {
            PRINT_ZERO("adjust_masks: missing grid_center_lon\n");
        } else {
            lon->get_dims()[0]->get_mask()->adjust(box, lat, lon);
        }
        lat = get_var(string("grid_corner_lat2"), false, true);
        lon = get_var(string("grid_corner_lon2"), false, true);
        if (!lat) {
            PRINT_ZERO("adjust_masks: missing grid_corner_lat\n");
        } else if (!lon) {
            PRINT_ZERO("adjust_masks: missing grid_corner_lon\n");
        } else {
            lon->get_dims()[0]->get_mask()->adjust(box, lat, lon);
        }
        lat = get_var(string("grid_edge_lat2"), false, true);
        lon = get_var(string("grid_edge_lon2"), false, true);
        if (!lat) {
            PRINT_ZERO("adjust_masks: missing grid_corner_lat\n");
        } else if (!lon) {
            PRINT_ZERO("adjust_masks: missing grid_corner_lon\n");
        } else {
            lon->get_dims()[0]->get_mask()->adjust(box, lat, lon);
        }
    }
}
 */

#else /* ADJUST_MASKS_WITHOUT_PRESERVATION */

/*
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
    
    var = dataset->get_var(var_name, false, true);
    dim = dataset->get_dim(dim_name);

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
*/


/*
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

    lat_var = dataset->get_var(string("grid_center_lat"), false, true);
    lon_var = dataset->get_var(string("grid_center_lon"), false, true);
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
*/


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
 */


#endif /* ADJUST_MASKS_WITHOUT_PRESERVATION */



