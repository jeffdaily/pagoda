#ifndef NETCDFVARIABLE_H_
#define NETCDFVARIABLE_H_

#include <iostream>
#include <string>
#include <vector>

using std::ostream;
using std::string;
using std::vector;

#include "AbstractVariable.H"
#include "DataType.H"

class Attribute;
class Dimension;
class NetcdfAttribute;
class NetcdfDimension;


/**
 * The pnetcdf implementation of the Variable.
 */
class NetcdfVariable : public AbstractVariable
{
    public:
        NetcdfVariable(NetcdfDataset *dataset, int varid);
        virtual ~NetcdfVariable();

        // virtuals from AbstractVariable
        virtual string get_name() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Attribute*> get_atts() const;
        virtual DataType get_type() const;
        virtual Array* read(Array *dst);
        virtual Array* read(int64_t record, Array *dst);

        virtual ostream& print(ostream &os) const;

        NetcdfDataset* get_dataset() const;
        int get_id() const;

    protected:
        NetcdfDataset *dataset;
        int id;
        string name;
        vector<NetcdfDimension*> dims;
        vector<NetcdfAttribute*> atts;
        DataType type;
};

#endif // NETCDFVARIABLE_H_
