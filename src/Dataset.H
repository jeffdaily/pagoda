#ifndef DATASET_H_
#define DATASET_H_

#include <iostream>
#include <string>
#include <vector>

#include "LatLonBox.H"
#include "Slice.H"

using std::ostream;
using std::string;
using std::vector;

class Attribute;
class Dimension;
class Grid;
class MaskMap;
class Variable;


/**
 * Open a file and access the Attributes, Variables, and Dimensions within.
 */
class Dataset
{
    public:
        static Dataset* open(const string &filename);

        virtual ~Dataset();

        virtual vector<Attribute*> get_atts() const = 0;
        virtual vector<Dimension*> get_dims() const = 0;
        virtual vector<Variable*> get_vars() const = 0;

        virtual vector<Grid*> get_grids() = 0;
        virtual Grid* get_grid() = 0;

        virtual Attribute* get_att(const string &name,
                bool ignore_case=false, bool within=false) const = 0;
        virtual Attribute* get_att(const vector<string> &names,
                bool ignore_case=false, bool within=false) const = 0;

        virtual Dimension* get_dim(const string &name,
                bool ignore_case=false, bool within=false) const = 0;

        virtual Variable* get_var(const string &name,
                bool ignore_case=false, bool within=false) const = 0;

        virtual void set_masks(MaskMap *masks) = 0;
        virtual void push_masks(MaskMap *masks) = 0;
        virtual MaskMap* pop_masks() = 0;
        virtual MaskMap* get_masks() const = 0;

        virtual void wait() = 0;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Dataset *dataset);

    protected:
        Dataset();
};

#endif // DATASET_H_
