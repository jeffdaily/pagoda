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

MaskMap::MaskMap()
    :   masks()
{
}


MaskMap::MaskMap(Dataset *dataset)
    :   masks()
{
    create_masks(dataset->get_dims());
    dataset->set_masks(this);
}


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


void MaskMap::modify(const vector<DimSlice> &slices)
{
    vector<DimSlice>::const_iterator slice_it;

    TIMING("Dataset::modify(vector<DimSlice>,vector<Dimension*>)");

    // we're iterating over the command-line specified slices to create masks
    for (slice_it=slices.begin(); slice_it!=slices.end(); ++slice_it) {
        DimSlice slice = *slice_it;
        string slice_name = slice.get_name();
        masks_t::const_iterator mask_it = masks.find(slice_name);

        if (mask_it == masks.end()) {
            pagoda::print_zero("Sliced dimension '%s' does not exist\n",
                    slice_name.c_str());
        } else {
            // modify the Mask based on the current Slice
            mask_it->second->modify(slice);
        }
    }
}


void MaskMap::modify(const LatLonBox &box, Grid *grid)
{
    if (grid->get_type() == GridType::GEODESIC) {
        Variable *lat = grid->get_cell_lat();
        Variable *lon = grid->get_cell_lon();
        Dimension *dim = lat->get_dims().at(0);
        modify(box, lat, lon, dim);
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
    Array *lat_array = lat->read();
    Array *lon_array = lon->read();
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
    vector<int64_t> masked_lo(1);
    vector<int64_t> masked_hi(1);
    vector<int64_t> topology_lo(2);
    vector<int64_t> topology_hi(2);
    int *mask_data;
    Array *topology_array;
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
    topology_array = topology->read();
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
