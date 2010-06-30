#ifndef NETCDFDATASET_H_
#define NETCDFDATASET_H_

#include <string>
#include <vector>

using std::string;
using std::vector;

#include "Dataset.H"

class Attribute;
class Dimension;
class MaskMap;
class NetcdfAttribute;
class NetcdfDimension;
class NetcdfVariable;
class Variable;


/**
 * The pnetcdf implementation of the Dataset.
 */
class NetcdfDataset : public Dataset
{
    public:
        NetcdfDataset(const string &filename);
        virtual ~NetcdfDataset();

        virtual vector<Attribute*> get_atts() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Variable*> get_vars() const;

        virtual void set_masks(MaskMap *masks);
        virtual MaskMap* get_masks() const;

        virtual ostream& print(ostream &os) const;

        NetcdfAttribute* get_att(size_t i) const;
        NetcdfDimension* get_dim(size_t i) const;
        NetcdfVariable*  get_var(size_t i) const;

        string get_filename() const;
        int get_id() const;
        NetcdfDimension* get_udim() const;

    protected:
        string filename;
        int ncid;
        int udim;
        vector<NetcdfAttribute*> atts;
        vector<NetcdfDimension*> dims;
        vector<NetcdfVariable*> vars;
        MaskMap *masks;
};

#endif // NETCDFDATASET_H_
