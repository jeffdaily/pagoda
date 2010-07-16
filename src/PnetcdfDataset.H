#ifndef PNETCDFDATASET_H_
#define PNETCDFDATASET_H_

#include <pnetcdf.h>

#include <string>
#include <vector>

using std::string;
using std::vector;

#include "AbstractDataset.H"

class Array;
class Attribute;
class Dimension;
class PnetcdfAttribute;
class PnetcdfDimension;
class PnetcdfVariable;
class Variable;


/**
 * The pnetcdf implementation of the Dataset.
 */
class PnetcdfDataset : public AbstractDataset
{
    public:
        PnetcdfDataset(const string &filename);
        virtual ~PnetcdfDataset();
        virtual void close();

        virtual vector<Attribute*> get_atts() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Variable*> get_vars() const;

        virtual void wait();

        virtual ostream& print(ostream &os) const;

        PnetcdfAttribute* get_att(size_t i) const;
        PnetcdfDimension* get_dim(size_t i) const;
        PnetcdfVariable*  get_var(size_t i) const;

        string get_filename() const;
        int get_id() const;
        PnetcdfDimension* get_udim() const;

        friend class PnetcdfVariable;

    protected:
        string filename;
        int ncid;
        int udim;
        vector<PnetcdfAttribute*> atts;
        vector<PnetcdfDimension*> dims;
        vector<PnetcdfVariable*> vars;

        vector<int> requests;
        vector<Array*> arrays_to_release;
        bool open;
};

#endif // PNETCDFDATASET_H_
