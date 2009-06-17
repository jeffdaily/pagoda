#include <pnetcdf.h>

#include "Dimension.H"
#include "Util.H"

Dimension::Dimension()
    :   name("")
    ,   size(0)
    ,   unlimited(false)
{
}


Dimension::Dimension(int ncid, int dimid)
    :   name("")
    ,   size(0)
    ,   unlimited(false)
{
    int udim;
    int err = ncmpi_inq_unlimdim(ncid, &udim);
    ERRNO_CHECK(err);
    char name_tmp[MAX_NAME];
    MPI_Offset size_tmp;
    err = ncmpi_inq_dim(ncid, dimid, name_tmp, &size_tmp);
    ERRNO_CHECK(err);
    name = string(name_tmp);
    size = size_tmp;
    unlimited = (udim == dimid);
}


Dimension::Dimension(string name, int64_t length, bool unlimited)
    :   name(name)
    ,   size(length)
    ,   unlimited(unlimited)
{
}


Dimension::Dimension(const Dimension &copy)
    :   name(copy.name)
    ,   size(copy.size)
    ,   unlimited(copy.unlimited)
{
}


Dimension::~Dimension()
{
}


Dimension& Dimension::operator = (const Dimension &other)
{
    name = other.name;
    size = other.size;
    unlimited = other.unlimited;
    return *this;
}


bool Dimension::operator == (const Dimension &other) const
{
    return name == other.name
            && size == other.size
            && unlimited == other.unlimited;
}


string Dimension::get_name() const
{
    return name;
}


int64_t Dimension::get_size() const
{
    return size;
}


bool Dimension::is_unlimited() const
{
    return unlimited;
}


ostream& operator << (ostream &os, const Dimension &other)
{
    os << "Dimension(";
    os << other.name << ",";
    os << other.size << ",";
    os << other.unlimited << ")";
    return os;
}

