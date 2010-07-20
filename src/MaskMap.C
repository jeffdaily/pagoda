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

#include "Array.H"
#include "Common.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "Grid.H"
#include "LatLonBox.H"
#include "Mask.H"
#include "MaskMap.H"
#include "NotImplementedException.H"
#include "Slice.H"
#include "Timing.H"
#include "Variable.H"


/**
 * Default constructor.
 */
MaskMap::MaskMap()
    :   masks()
    ,   cleared()
{
}


/**
 * Create Masks based on the Dimensions found in the given Dataset.
 *
 * @param[in] dataset the Dataset from which to get Dimensions
 */
MaskMap::MaskMap(Dataset *dataset)
    :   masks()
    ,   cleared()
{
    create_masks(dataset->get_dims());
    dataset->set_masks(this);
}


/**
 * Destuctor.
 *
 * Deletes all Masks.
 */
MaskMap::~MaskMap()
{
    for (masks_t::iterator it=masks.begin(); it!= masks.end(); ++it) {
        delete it->second;
    }
}


/**
 * Create a default Mask for each Dimension in the given Dataset.
 *
 * If a Mask does not yet exist for a Dimension, it is initialized to 1.
 * Otherwise, if a Mask already exists, it is left alone.
 *
 * @param[in] dataset the Dataset from which to get Dimensions
 */
void MaskMap::create_masks(const Dataset *dataset)
{
    TIMING("MaskMap::create_masks(Dataset*)");
    create_masks(dataset->get_dims());
}


/**
 * Create a default Mask for each given Dimension.
 *
 * If a Mask does not yet exist for a Dimension, it is initialized to 1.
 * Otherwise, if a Mask already exists, it is left alone.
 *
 * @param[in] dims the Dimensions for which Masks should be created
 */
void MaskMap::create_masks(const vector<Dimension*> dims)
{
    vector<Dimension*>::const_iterator dim_it;
    vector<Dimension*>::const_iterator dim_end;

    TIMING("MaskMap::create_masks(vector<Dimension*>)");

    for (dim_it=dims.begin(),dim_end=dims.end(); dim_it!=dim_end; ++dim_it) {
        Dimension *dim = *dim_it;
        Mask *mask = get_mask(dim);
    }
}


/**
 * Calls Mask::modify(DimSlice) for the given slice if associated Mask is
 * found.
 *
 * If an associated Mask is not found for the indicated Dimension, it is
 * reported to stderr but otherwise ignored.
 *
 * @param[in] slice the DimSlice to apply to associatd Masks
 */
void MaskMap::modify(const DimSlice &slice)
{
    string slice_name = slice.get_name();
    masks_t::const_iterator mask_it = masks.find(slice_name);

    TIMING("MaskMap::modify(DimSlice)");
    TRACER("MaskMap::modify(DimSlice)\n");

    if (mask_it == masks.end()) {
        pagoda::print_zero("Sliced dimension '%s' does not exist\n",
                slice_name.c_str());
    } else {
        // clear the Mask the first time only
        if (cleared.count(slice_name) == 0) {
            cleared.insert(slice_name);
            mask_it->second->clear();
        }
        // modify the Mask based on the current Slice
        mask_it->second->modify(slice);
    }
}


/**
 * Calls Mask::modify(DimSlice) for each given slice if associated Mask is
 * found.
 *
 * If an associated Mask is not found for the indicated Dimension, it is
 * reported to stderr but otherwise ignored.
 *
 * @param[in] slices the DimSlices to apply to associatd Masks
 */
void MaskMap::modify(const vector<DimSlice> &slices)
{
    vector<DimSlice>::const_iterator slice_it;

    TIMING("Dataset::modify(vector<DimSlice>,vector<Dimension*>)");

    // we're iterating over the command-line specified slices to create masks
    for (slice_it=slices.begin(); slice_it!=slices.end(); ++slice_it) {
        modify(*slice_it);
    }
}


void MaskMap::modify(const LatLonBox &box, Grid *grid)
{
    TIMING("MaskMap::modify(LatLonBox,Grid)");
    TRACER("MaskMap::modify(LatLonBox,Grid)\n");

    if (grid->get_type() == GridType::GEODESIC) {
        Variable *cell_lat = grid->get_cell_lat();
        Variable *cell_lon = grid->get_cell_lon();
        Variable *corner_lat = grid->get_corner_lat();
        Variable *corner_lon = grid->get_corner_lon();
        Variable *edge_lat = grid->get_edge_lat();
        Variable *edge_lon = grid->get_edge_lon();
        Variable *cell_corners = grid->get_cell_corners();
        Variable *cell_edges = grid->get_cell_edges();
        Dimension *cell_dim;
        Dimension *corner_dim;
        Dimension *edge_dim;

        if (cell_lat)        cell_dim = cell_lat->get_dims().at(0);
        else if (cell_lon)   cell_dim = cell_lon->get_dims().at(0);
        if (corner_lat)      corner_dim = corner_lat->get_dims().at(0);
        else if (corner_lat) corner_dim = corner_lon->get_dims().at(0);
        if (edge_lat)        edge_dim = edge_lat->get_dims().at(0);
        else if (edge_lat)   edge_dim = edge_lon->get_dims().at(0);

        if (cell_lat && cell_lon && cell_dim) {
            if (grid->is_radians()) {
                modify(box*RAD_PER_DEG, cell_lat, cell_lon, cell_dim);
            } else {
                modify(box, cell_lat, cell_lon, cell_dim);
            }
            if (corner_dim && cell_corners) {
                modify(cell_dim, corner_dim, cell_corners);
            } else {
                if (!corner_lat) {
                    pagoda::print_zero("grid corner lat missing\n");
                }
                if (!corner_lon) {
                    pagoda::print_zero("grid corner lon missing\n");
                }
                if (!corner_dim) {
                    pagoda::print_zero("grid corner dim missing\n");
                }
            }
            if (edge_dim && cell_edges) {
                modify(cell_dim, edge_dim, cell_edges);
            } else {
                if (!edge_lat) {
                    pagoda::print_zero("grid edge lat missing\n");
                }
                if (!edge_lon) {
                    pagoda::print_zero("grid edge lon missing\n");
                }
                if (!edge_dim) {
                    pagoda::print_zero("grid edge dim missing\n");
                }
            }
        } else {
            if (!cell_lat) {
                pagoda::print_zero("grid cell lat missing\n");
            }
            if (!cell_lon) {
                pagoda::print_zero("grid cell lon missing\n");
            }
            if (!cell_dim) {
                pagoda::print_zero("grid cell dim missing\n");
            }
        }
    }
}


void MaskMap::modify(const vector<LatLonBox> &boxes, Grid *grid)
{
    vector<LatLonBox>::const_iterator it;
    vector<LatLonBox>::const_iterator end;

    for (it=boxes.begin(),end=boxes.end(); it!=end; ++it) {
        modify(*it, grid);
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
void MaskMap::modify(const LatLonBox &box,
        const Variable *lat, const Variable *lon, Dimension *dim)
{
    Mask *mask = get_mask(dim);
    Array *lat_array;
    Array *lon_array;

    TIMING("MaskMap::modify(LatLonBox,Variable,Variable,Dimension)");
    TRACER("MaskMap::modify(LatLonBox,Variable,Variable,Dimension)\n");

    lat->get_dataset()->push_masks(NULL);
    lat_array = lat->read();
    lon_array = lon->read();
    lat->get_dataset()->pop_masks();

    // clear the Mask the first time only
    if (cleared.count(dim->get_name()) == 0) {
        cleared.insert(dim->get_name());
        mask->clear();
    }
    mask->modify(box, lat_array, lon_array);
    delete lat_array;
    delete lon_array;
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
void MaskMap::modify(
        const LatLonBox &box, const Variable *lat, const Variable *lon,
        Dimension *lat_dim, Dimension *lon_dim)
{
    throw NotImplementedException("MaskMap::modify(LatLonBox,Variable*,Variable*,Dimension*,Dimension*)");
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
void MaskMap::modify(
        Dimension *dim_masked, Dimension *dim_to_mask, const Variable *topology)
{
    Mask *mask = get_mask(dim_masked);
    Mask *to_mask = get_mask(dim_to_mask);
    int64_t connections = topology->get_shape().at(1);
    int64_t mask_local_size = mask->get_local_size();
    vector<int64_t> masked_lo(1,0);
    vector<int64_t> masked_hi(1,0);
    vector<int64_t> topology_lo(2,0);
    vector<int64_t> topology_hi(2,0);
    int *mask_data;
    Array *topology_array;
    int *topology_data;
    vector<int> topology_buffer;
    vector<int64_t> topology_subscripts;

    // clear the Mask the first time only
    if (cleared.count(dim_to_mask->get_name()) == 0) {
        cleared.insert(dim_to_mask->get_name());
        to_mask->clear();
    }

    // regardless of whether the mask owns the data, the topology variable
    // must be read in, and in its entirety (no subsetting)
    dim_masked->get_dataset()->push_masks(NULL);
    topology_array = topology->read();
    dim_masked->get_dataset()->pop_masks();

    if (!mask->owns_data()) {
        // bail if this process doesn't own any of the mask
        delete topology_array;
        return;
    }

    mask->get_distribution(masked_lo, masked_hi);
    topology_lo[0] = masked_lo.at(0);
    topology_lo[1] = 0;
    topology_hi[0] = masked_lo.at(0) + mask_local_size-1;
    topology_hi[1] = connections-1;
    
    // get portion of the topology local to the mask
    topology_data = (int*)topology_array->get(topology_lo, topology_hi);
    
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
    delete topology_array;
    delete [] topology_data;
}


Mask* MaskMap::get_mask(const Dimension *dim)
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


Mask* MaskMap::operator [] (const Dimension *dim)
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
