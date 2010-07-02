#ifndef MASKMAP_H_
#define MASKMAP_H_

#include <iostream>
#include <map>
#include <string>
#include <vector>

using std::map;
using std::ostream;
using std::string;
using std::vector;

#include "LatLonBox.H"
#include "Slice.H"

class Dataset;
class Dimension;
class Mask;

typedef map<string,Mask*> masks_t;


/**
 * A Mask container.
 *
 * All Mask information is kept here and can be passed around as a container.
 * The intent is to support multiple containers for Masks which can be set or
 * unset from a Dataset.
 */
class MaskMap
{
    public:
        MaskMap();
        virtual ~MaskMap();

        void create_masks(const Dataset *dataset);

        void modify_masks(const vector<DimSlice> &slices,
                const vector<Dimension*> &dims);
        void modify_masks(const LatLonBox &box,
                const Array *lat, const Array *lon, Dimension *dim);
        void modify_masks(const LatLonBox &box,
                const Array *lat, const Array *lon,
                Dimension *lat_dim, Dimension *lon_dim);
        void modify_masks(Dimension *masked, Dimension *to_mask,
                const Array *topology);

        Mask* get_mask(Dimension *dim);
        Mask* operator [] (Dimension *dim);

        ostream& print(ostream &os) const;
        friend ostream& operator << (ostream &os, const MaskMap &maskmap);
        friend ostream& operator << (ostream &os, const MaskMap *maskmap);

    protected:
        Mask* get_mask(const string &name, const Dimension *dim);

        masks_t masks;
};

#endif /* MASKMAP_H_ */
