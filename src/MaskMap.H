#ifndef MASKMAP_H_
#define MASKMAP_H_

#include <stdint.h>

#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

using std::map;
using std::ostream;
using std::set;
using std::string;
using std::vector;

#include "CoordHyperslab.H"
#include "IndexHyperslab.H"
#include "LatLonBox.H"

class Array;
class Dataset;
class Dimension;
class Grid;
class Variable;

typedef map<string,Array*> masks_t;
typedef map<string,int64_t> sizes_t;
typedef set<string> cleared_t;


/**
 * A mask container.
 *
 * All mask information is kept here and can be passed around as a container.
 * The intent is to support multiple containers for masks which can be set or
 * unset from a Dataset.
 *
 * Masks cannot be shared between MaskMap instances.  The destructor will
 * delete all masks associated with the MaskMap instance.
 *
 * Mask instances are either created up front or can be seeded.  Seeding a
 * mask will store the name and size of the mask without allocating any memory
 * for it.  A mask instance will only be created upon request via get_mask()
 * or the operator[].  A seeded mask will return false for has_mask().
 *
 * Mask instances are initially created with all bits reset (set to 1)
 * effectively the same as having no mask at all.  This initial condition must
 * be cleared before modifying any masks, otherwise the operations will simply
 * overwrite the 1s with additional 1s.  The clearing operation happens once
 * by default for the modify functions defined here.
 */
class MaskMap
{
    public:
        MaskMap();
        MaskMap(Dataset *dataset, bool seed=true);
        virtual ~MaskMap();

        void create_mask(const Dimension *dim);
        void create_mask(const string &name, int64_t size);
        void create_masks(const Dataset *dataset);
        void create_masks(const vector<Dimension*> &dims);
 
        void seed_mask(const Dimension *dim);
        void seed_mask(const string &name, int64_t size);
        void seed_masks(const Dataset *dataset);
        void seed_masks(const vector<Dimension*> &dims);

        void modify(const IndexHyperslab &hyperslab);
        void modify(const vector<IndexHyperslab> &hyperslabs);
        void modify(const CoordHyperslab &hyperslab, Grid *grid);
        void modify(const vector<CoordHyperslab> &hyperslabs, Grid *grid);
        void modify(const LatLonBox &box, Grid *grid);
        void modify(const vector<LatLonBox> &boxes, Grid *grid);
        void modify(const string &name, Array *values);

        void clear_mask(const string &name);
        bool has_mask(const string &name) const;
        Array* get_mask(const string &name);
        Array* operator [](const string &name);

        void clear_mask(const Dimension *dim);
        bool has_mask(const Dimension *dim) const;
        Array* get_mask(const Dimension *dim);
        Array* operator [](const Dimension *dim);

        ostream& print(ostream &os) const;
        friend ostream& operator << (ostream &os, const MaskMap &maskmap);
        friend ostream& operator << (ostream &os, const MaskMap *maskmap);

    protected:
        Array* get_mask(const string &name, const Dimension *dim);

        void modify(const LatLonBox &box,
                    const Variable *lat, const Variable *lon,
                    Dimension *dim);
        void modify(const LatLonBox &box,
                    const Variable *lat, const Variable *lon,
                    Dimension *lat_dim, Dimension *lon_dim);
        void modify(Dimension *masked, Dimension *to_mask,
                    const Variable *topology);

        masks_t masks;
        sizes_t sizes;
        cleared_t cleared;
};

#endif /* MASKMAP_H_ */
