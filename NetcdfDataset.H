#ifndef NETCDFDATASET_H_
#define NETCDFDATASET_H_

#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "Dataset.H"

class Attribute;
class Dimension;
class Variable;


class NetcdfDataset : public Dataset
{
    public:
        NetcdfDataset(const string &filename);

        virtual vector<Attribute*> get_atts();
        virtual vector<Dimension*> get_dims();
        virtual vector<Variable*> get_vars();

        virtual ostream& print(ostream &os) const;

    private:
        int ncid;
        int udim;
        vector<Attribute*> atts;
        vector<Dimension*> dims;
        vector<Variable*> vars;
};

#endif // NETCDFDATASET_H_
