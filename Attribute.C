#include "Attribute.H"
#include "Util.H"


Attribute::Attribute()
    :   name("")
    ,   type(DataType::CHAR)
    ,   count(0)
    ,   global(false)
    ,   file("")
    ,   variable("")
{
}


Attribute::Attribute(int ncid, int attid, int varid, string file)
    :   name("")
    ,   type(DataType::CHAR)
    ,   count(0)
    ,   global(varid == NC_GLOBAL)
    ,   file(file)
    ,   variable("")
{
    int err;
    if (!global) {
        char var_name_tmp[MAX_NAME];
        err = ncmpi_inq_varname(ncid, varid, var_name_tmp);
        ERRNO_CHECK(err);
        variable = string(var_name_tmp);
    }
    char att_name_tmp[MAX_NAME];
    err = ncmpi_inq_attname(ncid, varid, attid, att_name_tmp);
    ERRNO_CHECK(err);
    name = string(att_name_tmp);
    nc_type type_tmp;
    MPI_Offset len;
    err = ncmpi_inq_att(ncid, varid, name.c_str(), &type_tmp, &len);
    count = len;
    type = type_tmp;
}


Attribute::Attribute(const Attribute &copy)
    :   name(copy.name)
    ,   type(copy.type)
    ,   count(copy.count)
    ,   global(copy.global)
    ,   file(copy.file)
    ,   variable(copy.variable)
{
}


Attribute::~Attribute()
{
}


string Attribute::get_name() const
{
    return name;
}


DataType Attribute::get_type() const
{
    return type;
}


long Attribute::get_count() const
{
    return count;
}


bool Attribute::is_global() const
{
    return global;
}


bool Attribute::operator == (const Attribute &other) const
{
    return name == other.name
            && type == other.type
            && count == other.count
            && global == other.global;
}


ostream& operator << (ostream &os, const Attribute &other)
{
    os << other.name << " = " ;
    return os;
}

