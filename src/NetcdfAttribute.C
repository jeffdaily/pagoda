#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

#include "Common.H"
#include "DataType.H"
#include "NetcdfAttribute.H"
#include "NetcdfDataset.H"
#include "NetcdfError.H"
#include "NetcdfVariable.H"
#include "Pnetcdf.H"
#include "Util.H"
#include "Values.H"
#include "Timing.H"
#include "TypedValues.H"


NetcdfAttribute::NetcdfAttribute(
        NetcdfDataset *dataset,
        int attid,
        NetcdfVariable *var)
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
    char att_name_tmp[MAX_NAME];
    nc_type type_tmp;
    MPI_Offset len_mpi;
    size_t len;

    TIMING("NetcdfAttribute::NetcdfAttribute(...)");

    ncmpi::inq_attname(ncid, varid, attid, att_name_tmp);
    name = string(att_name_tmp);
    ncmpi::inq_att(ncid, varid, name.c_str(), &type_tmp, &len_mpi);
    len = len_mpi;
    type = type_tmp;
#define get_attr_values(DATA_TYPE, C_TYPE) \
    if (type == DATA_TYPE) { \
        C_TYPE data[len]; \
        ncmpi::get_att(ncid, varid, name.c_str(), data); \
        values = new TypedValues<C_TYPE>(data, len); \
    } else
    get_attr_values(DataType::CHAR,   char)
    get_attr_values(DataType::SCHAR,  signed char)
    get_attr_values(DataType::UCHAR,  unsigned char)
    get_attr_values(DataType::SHORT,  short)
    get_attr_values(DataType::INT,    int)
    get_attr_values(DataType::FLOAT,  float)
    get_attr_values(DataType::DOUBLE, double)
    {
        throw DataTypeException("DataType not handled", type);
    }
#undef get_attr_values
}


NetcdfAttribute::~NetcdfAttribute()
{
    TIMING("NetcdfAttribute::~NetcdfAttribute()");
    delete values;
}


string NetcdfAttribute::get_name() const
{
    TIMING("NetcdfAttribute::get_name()");
    return name;
}


DataType NetcdfAttribute::get_type() const
{
    TIMING("NetcdfAttribute::get_type()");
    return type;
}


Values* NetcdfAttribute::get_values() const
{
    TIMING("NetcdfAttribute::get_values()");
    return values;
}


Dataset* NetcdfAttribute::get_dataset() const
{
    return dataset;
}


Variable* NetcdfAttribute::get_var() const
{
    return var;
}


NetcdfDataset* NetcdfAttribute::get_netcdf_dataset() const
{
    TIMING("NetcdfAttribute::get_netcdf_dataset()");
    return dataset;
}


NetcdfVariable* NetcdfAttribute::get_netcdf_var() const
{
    TIMING("NetcdfAttribute::get_netcdf_var()");
    return var;
}


int NetcdfAttribute::get_id() const
{
    TIMING("NetcdfAttribute::get_id()");
    return id;
}
