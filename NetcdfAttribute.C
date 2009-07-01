#include <pnetcdf.h>

#include "NetcdfAttribute.H"
#include "Util.H"
#include "Values.H"
#include "TypedValues.H"


NetcdfAttribute::NetcdfAttribute(int ncid, int attid, int varid)
    :   ncid(ncid)
    ,   id(attid)
    ,   varid(varid)
    ,   name("")
    ,   type(DataType::CHAR)
    ,   global(varid == NC_GLOBAL)
    ,   values(NULL)
{
    int err;
    char att_name_tmp[MAX_NAME];
    err = ncmpi_inq_attname(ncid, varid, attid, att_name_tmp);
    ERRNO_CHECK(err);
    name = string(att_name_tmp);
    nc_type type_tmp;
    MPI_Offset len;
    err = ncmpi_inq_att(ncid, varid, name.c_str(), &type_tmp, &len);
    type = type_tmp;
#define get_attr_values(DT, CT, NAME) \
    if (type == DT) { \
        CT data[len]; \
        err = ncmpi_get_att_##NAME(ncid, varid, name.c_str(), data); \
        ERRNO_CHECK(err); \
        values = new TypedValues<CT>(data, len); \
    } else
    get_attr_values(DataType::BYTE, signed char, schar)
    get_attr_values(DataType::CHAR, char, text)
    get_attr_values(DataType::SHORT, short, short)
    get_attr_values(DataType::INT, int, int)
    get_attr_values(DataType::LONG, long, long)
    get_attr_values(DataType::FLOAT, float, float)
    get_attr_values(DataType::DOUBLE, double, double)
#undef get_attr_values
    ; // because of last "else" in macro
}


NetcdfAttribute::NetcdfAttribute(const NetcdfAttribute &copy)
    :   ncid(copy.ncid)
    ,   id(copy.id)
    ,   varid(copy.varid)
    ,   name(copy.name)
    ,   type(copy.type)
    ,   global(copy.global)
    ,   values(copy.values->clone())
{
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

