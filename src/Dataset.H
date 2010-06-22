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


/**
 * Open a file and access the Attributes, Variables, and Dimensions within.
 */
class Dataset
{
    public:
        static Dataset* open(const string &filename);

        virtual ~Dataset();

        virtual vector<Attribute*> get_atts() = 0;
        virtual vector<Dimension*> get_dims() const = 0;
        virtual vector<Variable*> get_vars() = 0;

        virtual Attribute* find_att(const string &name,
                bool ignore_case=false, bool within=false);
        virtual Attribute* find_att(const vector<string> &names,
                bool ignore_case=false, bool within=false);

        virtual Dimension* find_dim(const string &name,
                bool ignore_case=false, bool within=false);

        virtual Variable* find_var(const string &name,
                bool ignore_case=false, bool within=false);

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Dataset *dataset);

        virtual void decorate();

    protected:
        Dataset();
};

#endif // DATASET_H_
