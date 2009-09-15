#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

#include "Common.H"
#include "NetcdfAttribute.H"
#include "NetcdfDataset.H"
#include "NetcdfError.H"
#include "NetcdfVariable.H"
#include "Util.H"
#include "Values.H"
#include "TypedValues.H"


NetcdfAttribute::NetcdfAttribute(
        NetcdfDataset *dataset,
        int attid,
        NetcdfVariable *var)
    :   Attribute()
    ,   dataset(dataset)
    ,   id(attid)
    ,   var(var)
    ,   name("")
    ,   type(NC_CHAR)
    ,   global(var == NULL)
    ,   values(NULL)
{
    int err = 0;
    int ncid = dataset->get_id();
    int varid = (var == NULL) ? NC_GLOBAL : var->get_id();
    char att_name_tmp[MAX_NAME];
    nc_type type_tmp;
    MPI_Offset len_mpi;
    size_t len;

    err = ncmpi_inq_attname(ncid, varid, attid, att_name_tmp);
    ERRNO_CHECK(err);
    name = string(att_name_tmp);
    err = ncmpi_inq_att(ncid, varid, name.c_str(), &type_tmp, &len_mpi);
    ERRNO_CHECK(err);
    len = len_mpi;
    type = type_tmp;
#define get_attr_values(DATA_TYPE, C_TYPE, NAME) \
    if (DATA_TYPE == type_tmp) { \
        C_TYPE data[len]; \
        err = ncmpi_get_att_##NAME(ncid, varid, name.c_str(), data); \
        ERRNO_CHECK(err); \
        values = new TypedValues<C_TYPE>(data, len); \
    } else
    get_attr_values(NC_BYTE, signed char, schar)
    get_attr_values(NC_CHAR, char, text)
    get_attr_values(NC_SHORT, short, short)
    get_attr_values(NC_INT, int, int)
    get_attr_values(NC_FLOAT, float, float)
    get_attr_values(NC_DOUBLE, double, double)
    ; // because of last "else" in macro
#undef get_attr_values
}


NetcdfAttribute::~NetcdfAttribute()
{
    delete values;
}


string NetcdfAttribute::get_name() const
{
    return name;
}


DataType NetcdfAttribute::get_type() const
{
    return type;
}


bool NetcdfAttribute::is_global() const
{
    return global;
}


Values* NetcdfAttribute::get_values() const
{
    return values;
}


NetcdfDataset* NetcdfAttribute::get_dataset() const
{
    return dataset;
}


int NetcdfAttribute::get_id() const
{
    return id;
}


NetcdfVariable* NetcdfAttribute::get_var() const
{
    return var;
}

