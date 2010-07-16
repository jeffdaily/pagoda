#ifndef NETCDFDATASET_H_
#define NETCDFDATASET_H_

#include <pnetcdf.h>

#include <string>
#include <vector>

using std::string;
using std::vector;

#include "AbstractDataset.H"

class Array;
class Attribute;
class Dimension;
class NetcdfAttribute;
class NetcdfDimension;
class NetcdfVariable;
class Variable;


/**
 * The pnetcdf implementation of the Dataset.
 */
class NetcdfDataset : public AbstractDataset
{
    public:
        NetcdfDataset(const string &filename);
        virtual ~NetcdfDataset();
        virtual void close();

        virtual vector<Attribute*> get_atts() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Variable*> get_vars() const;

        virtual void wait();

        virtual ostream& print(ostream &os) const;

        NetcdfAttribute* get_att(size_t i) const;
        NetcdfDimension* get_dim(size_t i) const;
        NetcdfVariable*  get_var(size_t i) const;

        string get_filename() const;
        int get_id() const;
        NetcdfDimension* get_udim() const;

        friend class NetcdfVariable;

    protected:
        string filename;
        int ncid;
        int udim;
        vector<NetcdfAttribute*> atts;
        vector<NetcdfDimension*> dims;
        vector<NetcdfVariable*> vars;

        vector<int> requests;
        vector<Array*> arrays_to_release;
        bool open;
};

#endif // NETCDFDATASET_H_
