#ifndef NETCDFATTRIBUTE_H_
#define NETCDFATTRIBUTE_H_

#include <iostream>
#include <string>

using std::ostream;
using std::string;

#include "Attribute.H"
#include "DataType.H"

class NetcdfDataset;
class NetcdfVariable;
class Values;


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
        virtual bool is_global() const;
        virtual Values* get_values() const;

        NetcdfDataset* get_dataset() const;
        int get_id() const;
        NetcdfVariable* get_var() const;

    protected:
        NetcdfDataset *dataset;
        int id;
        NetcdfVariable *var;
        string name;
        DataType type;
        bool global;
        Values *values;
};

#endif // NETCDFATTRIBUTE_H_
