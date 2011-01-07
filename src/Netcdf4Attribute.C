#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <netcdf.h>

#include <vector>

#include "Common.H"
#include "DataType.H"
#include "Netcdf4Attribute.H"
#include "Netcdf4Dataset.H"
#include "Netcdf4Error.H"
#include "Netcdf4Variable.H"
#include "Netcdf4.H"
#include "Util.H"
#include "Values.H"
#include "Timing.H"
#include "TypedValues.H"

using std::vector;


Netcdf4Attribute::Netcdf4Attribute(
    Netcdf4Dataset *dataset,
    int attid,
    Netcdf4Variable *var)
    :   Attribute()
    ,   dataset(dataset)
    ,   var(var)
    ,   id(attid)
    ,   name("")
    ,   type(DataType::CHAR)
    ,   values(NULL)
{
    int ncid = dataset->get_id();
    int varid = (var == NULL) ? NC_GLOBAL : var->get_id();

    TIMING("Netcdf4Attribute::Netcdf4Attribute(...)");

    name = nc::inq_attname(ncid, varid, attid);
    type = Netcdf4Dataset::to_dt(nc::inq_atttype(ncid, varid, name));
#define get_attr_values(DATA_TYPE, C_TYPE) \
    if (type == DATA_TYPE) { \
        vector<C_TYPE> data; \
        nc::get_att(ncid, varid, name, data); \
        values = new TypedValues<C_TYPE>(data); \
    } else
    get_attr_values(DataType::CHAR,   char)
    get_attr_values(DataType::SCHAR,  signed char)
    get_attr_values(DataType::UCHAR,  unsigned char)
    get_attr_values(DataType::SHORT,  short)
    get_attr_values(DataType::INT,    int)
    get_attr_values(DataType::LONG,   long)
    get_attr_values(DataType::FLOAT,  float)
    get_attr_values(DataType::DOUBLE, double) {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
#undef get_attr_values
}


Netcdf4Attribute::~Netcdf4Attribute()
{
    TIMING("Netcdf4Attribute::~Netcdf4Attribute()");
    delete values;
}


string Netcdf4Attribute::get_name() const
{
    TIMING("Netcdf4Attribute::get_name()");
    return name;
}


DataType Netcdf4Attribute::get_type() const
{
    TIMING("Netcdf4Attribute::get_type()");
    return type;
}


Values* Netcdf4Attribute::get_values() const
{
    TIMING("Netcdf4Attribute::get_values()");
    return values;
}


Dataset* Netcdf4Attribute::get_dataset() const
{
    return dataset;
}


Variable* Netcdf4Attribute::get_var() const
{
    return var;
}


Netcdf4Dataset* Netcdf4Attribute::get_netcdf_dataset() const
{
    TIMING("Netcdf4Attribute::get_netcdf_dataset()");
    return dataset;
}


Netcdf4Variable* Netcdf4Attribute::get_netcdf_var() const
{
    TIMING("Netcdf4Attribute::get_netcdf_var()");
    return var;
}


int Netcdf4Attribute::get_id() const
{
    TIMING("Netcdf4Attribute::get_id()");
    return id;
}
