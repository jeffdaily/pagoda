#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

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
    char att_name_tmp[MAX_NAME];
    nc_type type_tmp;
    MPI_Offset len_mpi;
    size_t len;

    TIMING("PnetcdfAttribute::PnetcdfAttribute(...)");

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
