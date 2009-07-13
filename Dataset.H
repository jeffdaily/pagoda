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

        virtual vector<Attribute*>& get_atts() = 0;
        virtual vector<Dimension*>& get_dims() = 0;
        virtual vector<Variable*>& get_vars() = 0;

        virtual Attribute* find_att(const string &name,
                bool ignore_case=true);
        virtual Attribute* find_att(const vector<string> &names,
                bool ignore_case=true);

        virtual Dimension* find_dim(const string &name,
                bool ignore_case=false);

        virtual Variable* find_var(const string &name,
                bool ignore_case=false);

        virtual void create_masks(const vector<DimSlice> &slices);
        virtual void create_masks(const LatLonBox &box);

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Dataset *dataset);

    protected:
        void decorate();
};

#endif // DATASET_H_
