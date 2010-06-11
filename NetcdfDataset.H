#ifndef NETCDFDATASET_H_
#define NETCDFDATASET_H_

#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "Dataset.H"

class Attribute;
class Dimension;
class NetcdfAttribute;
class NetcdfDimension;
class NetcdfVariable;
class Variable;


class NetcdfDataset : public Dataset
{
    public:
        NetcdfDataset(const string &filename);
        virtual ~NetcdfDataset();

        virtual vector<Attribute*> get_atts();
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Variable*> get_vars();

        virtual ostream& print(ostream &os) const;

        NetcdfAttribute* get_att(size_t i) const;
        NetcdfDimension* get_dim(size_t i) const;
        NetcdfVariable*  get_var(size_t i) const;

        string get_filename() const;
        int get_id() const;
        NetcdfDimension* get_udim() const;

    protected:
        virtual void decorate_set(const vector<Variable*> &vars) ;

        string filename;
        int ncid;
        int udim;
        vector<NetcdfAttribute*> atts;
        vector<NetcdfDimension*> dims;
        vector<NetcdfVariable*> vars;
        vector<Variable*> decorated;
};

#endif // NETCDFDATASET_H_
