#include <pnetcdf.h>

#include "NetcdfAttribute.H"
#include "NetcdfDataset.H"
#include "NetcdfVariable.H"
#include "Util.H"
#include "Values.H"
#include "TypedValues.H"


NetcdfAttribute::NetcdfAttribute(NetcdfDataset *dataset, int attid, NetcdfVariable *var)
    :   Attribute()
    ,   dataset(dataset)
    ,   id(attid)
    ,   var(var)
    ,   name("")
    ,   type(DataType::CHAR)
    ,   global(var == NULL)
    ,   values(NULL)
{
    int err;
    int ncid = dataset->get_id();
    int varid = (var == NULL) ? NC_GLOBAL : var->get_id();
    char att_name_tmp[MAX_NAME];
    err = ncmpi_inq_attname(ncid, varid, attid, att_name_tmp);
    ERRNO_CHECK(err);
    name = string(att_name_tmp);
    nc_type type_tmp;
    MPI_Offset len;
    err = ncmpi_inq_att(ncid, varid, name.c_str(), &type_tmp, &len);
    ERRNO_CHECK(err);
    type = type_tmp;
#define get_attr_values(DT, CT, NAME) \
    if (type == DT) { \
        CT data[len]; \
        err = ncmpi_get_att_##NAME(ncid, varid, name.c_str(), data); \
        ERRNO_CHECK(err); \
        values = new TypedValues<CT>(data, len); \
    } else
    get_attr_values(DataType::BYTE, unsigned char, uchar)
    get_attr_values(DataType::CHAR, char, text)
    get_attr_values(DataType::SHORT, short, short)
    get_attr_values(DataType::INT, int, int)
    get_attr_values(DataType::FLOAT, float, float)
    get_attr_values(DataType::DOUBLE, double, double)
#undef get_attr_values
    ; // because of last "else" in macro
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


size_t NetcdfAttribute::get_count() const
{
    return values->size();
}


bool NetcdfAttribute::is_global() const
{
    return global;
}


Values* NetcdfAttribute::get_values() const
{
    return values;
}


ostream& NetcdfAttribute::print(ostream &os) const
{
    os << name << "(" << get_type() << ") = " << values;
    return os;
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

