#ifndef DATASET_H_
#define DATASET_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "LatLonBox.H"
#include "Slice.H"

class Attribute;
class Dimension;
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
        virtual void adjust_masks(const vector<DimSlice> &slices);
        virtual void adjust_masks(const LatLonBox &box);

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Dataset *dataset);

    protected:
        virtual void decorate();
        virtual void decorate_set(const vector<Variable*> &vars) = 0;
};

#endif // DATASET_H_
