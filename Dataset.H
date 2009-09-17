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
class Mask;
class Variable;


class Dataset
{
    public:
        static Dataset* open(const string &filename);

        Dataset();
        virtual ~Dataset();

        virtual vector<Attribute*> get_atts() = 0;
        virtual vector<Dimension*> get_dims() = 0;
        virtual vector<Variable*> get_vars() = 0;

        virtual Attribute* find_att(const string &name,
                bool ignore_case=false, bool within=false);
        virtual Attribute* find_att(const vector<string> &names,
                bool ignore_case=false, bool within=false);

        virtual Dimension* find_dim(const string &name,
                bool ignore_case=false, bool within=false);

        virtual Variable* find_var(const string &name,
                bool ignore_case=false, bool within=false);

        virtual void create_masks();
        virtual void populate_masks(const vector<Mask*> &masks);
        virtual void adjust_masks(const vector<DimSlice> &slices);
        virtual void adjust_masks(const LatLonBox &box);

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Dataset *dataset);

        virtual void decorate();

    protected:
        virtual void decorate_set(const vector<Variable*> &vars) = 0;
};

#endif // DATASET_H_
