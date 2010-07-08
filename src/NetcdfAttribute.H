#ifndef NETCDFATTRIBUTE_H_
#define NETCDFATTRIBUTE_H_

#include <iostream>
#include <string>

using std::ostream;
using std::string;

#include "Attribute.H"
#include "DataType.H"

class Dataset;
class NetcdfDataset;
class NetcdfVariable;
class Values;
class Variable;


/**
 * The pnetcdf implementation of an Attribute.
 */
class NetcdfAttribute : public Attribute
{
    public:
        NetcdfAttribute(NetcdfDataset *dataset, int attid,
                NetcdfVariable *var=NULL);
        virtual ~NetcdfAttribute();

        virtual string get_name() const;
        virtual DataType get_type() const;
        virtual Values* get_values() const;

        virtual Dataset* get_dataset() const;
        virtual Variable* get_var() const;

        NetcdfDataset* get_netcdf_dataset() const;
        NetcdfVariable* get_netcdf_var() const;
        int get_id() const;

    protected:
        NetcdfDataset *dataset;
        NetcdfVariable *var;
        int id;
        string name;
        DataType type;
        Values *values;
};

#endif // NETCDFATTRIBUTE_H_
