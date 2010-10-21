#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

#include <vector>

#include "Common.H"
#include "DataType.H"
#include "PnetcdfAttribute.H"
#include "PnetcdfDataset.H"
#include "PnetcdfError.H"
#include "PnetcdfVariable.H"
#include "Pnetcdf.H"
#include "Util.H"
#include "Values.H"
#include "Timing.H"
#include "TypedValues.H"

using std::vector;


PnetcdfAttribute::PnetcdfAttribute(
        PnetcdfDataset *dataset,
        int attid,
        PnetcdfVariable *var)
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
    nc_type type_tmp;
    MPI_Offset len_mpi;

    TIMING("PnetcdfAttribute::PnetcdfAttribute(...)");

    name = ncmpi::inq_attname(ncid, varid, attid);
    type = ncmpi::inq_atttype(ncid, varid, name);
#define get_attr_values(DATA_TYPE, C_TYPE) \
    if (type == DATA_TYPE) { \
        vector<C_TYPE> data; \
        ncmpi::get_att(ncid, varid, name, data); \
        values = new TypedValues<C_TYPE>(data); \
    } else
    get_attr_values(DataType::CHAR,   char)
    get_attr_values(DataType::SCHAR,  signed char)
    get_attr_values(DataType::UCHAR,  unsigned char)
    get_attr_values(DataType::SHORT,  short)
    get_attr_values(DataType::INT,    int)
    get_attr_values(DataType::FLOAT,  float)
    get_attr_values(DataType::DOUBLE, double)
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
#undef get_attr_values
}


PnetcdfAttribute::~PnetcdfAttribute()
{
    TIMING("PnetcdfAttribute::~PnetcdfAttribute()");
    delete values;
}


string PnetcdfAttribute::get_name() const
{
    TIMING("PnetcdfAttribute::get_name()");
    return name;
}


DataType PnetcdfAttribute::get_type() const
{
    TIMING("PnetcdfAttribute::get_type()");
    return type;
}


Values* PnetcdfAttribute::get_values() const
{
    TIMING("PnetcdfAttribute::get_values()");
    return values;
}


Dataset* PnetcdfAttribute::get_dataset() const
{
    return dataset;
}


Variable* PnetcdfAttribute::get_var() const
{
    return var;
}


PnetcdfDataset* PnetcdfAttribute::get_netcdf_dataset() const
{
    TIMING("PnetcdfAttribute::get_netcdf_dataset()");
    return dataset;
}


PnetcdfVariable* PnetcdfAttribute::get_netcdf_var() const
{
    TIMING("PnetcdfAttribute::get_netcdf_var()");
    return var;
}


int PnetcdfAttribute::get_id() const
{
    TIMING("PnetcdfAttribute::get_id()");
    return id;
}
