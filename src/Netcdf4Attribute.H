#ifndef NETCDF4ATTRIBUTE_H_
#define NETCDF4ATTRIBUTE_H_

#include <iostream>
#include <string>

using std::ostream;
using std::string;

#include "Attribute.H"
#include "DataType.H"

class Dataset;
class Netcdf4Dataset;
class Netcdf4Variable;
class Values;
class Variable;


/**
 * The netcdf4 implementation of an Attribute.
 */
class Netcdf4Attribute : public Attribute
{
    public:
        Netcdf4Attribute(Netcdf4Dataset *dataset, int attid,
                         Netcdf4Variable *var=NULL);
        virtual ~Netcdf4Attribute();

        virtual string get_name() const;
        virtual DataType get_type() const;
        virtual Values* get_values() const;

        virtual Dataset* get_dataset() const;
        virtual Variable* get_var() const;

        Netcdf4Dataset* get_netcdf_dataset() const;
        Netcdf4Variable* get_netcdf_var() const;
        int get_id() const;

    protected:
        Netcdf4Dataset *dataset;
        Netcdf4Variable *var;
        int id;
        string name;
        DataType type;
        Values *values;
};

#endif // NETCDF4ATTRIBUTE_H_
