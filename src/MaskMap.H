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
class Grid;
class Mask;
class Variable;

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
        MaskMap(Dataset *dataset);
        virtual ~MaskMap();

        void create_masks(const Dataset *dataset);
        void create_masks(const vector<Dimension*> dims);

        void modify(const vector<DimSlice> &slices);
        void modify(const LatLonBox &box, Grid *grid);

        Mask* get_mask(const Dimension *dim);
        Mask* operator [] (const Dimension *dim);

        ostream& print(ostream &os) const;
        friend ostream& operator << (ostream &os, const MaskMap &maskmap);
        friend ostream& operator << (ostream &os, const MaskMap *maskmap);

    protected:
        Mask* get_mask(const string &name, const Dimension *dim);

        void modify(const LatLonBox &box,
                const Variable *lat, const Variable *lon,
                Dimension *dim);
        void modify(const LatLonBox &box,
                const Variable *lat, const Variable *lon,
                Dimension *lat_dim, Dimension *lon_dim);
        void modify(Dimension *masked, Dimension *to_mask,
                const Variable *topology);

        masks_t masks;
};

#endif /* MASKMAP_H_ */
