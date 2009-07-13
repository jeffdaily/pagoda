#include <pnetcdf.h>

#include "NetcdfDimension.H"
#include "Util.H"


NetcdfDimension::NetcdfDimension(int ncid, int dimid)
    :   Dimension()
    ,   ncid(ncid)
    ,   id(dimid)
    ,   name("")
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


NetcdfDimension::NetcdfDimension(const NetcdfDimension &copy)
    :   Dimension(copy)
    ,   ncid(copy.ncid)
    ,   id(copy.id)
    ,   name(copy.name)
    ,   size(copy.size)
    ,   unlimited(copy.unlimited)
{
}


NetcdfDimension::~NetcdfDimension()
{
}


NetcdfDimension& NetcdfDimension::operator = (const NetcdfDimension &other)
{
    if (&other != this) {
        this->Dimension::operator=(other);
        ncid = other.ncid;
        id = other.id;
        name = other.name;
        size = other.size;
        unlimited = other.unlimited;
    }
    return *this;
}


string NetcdfDimension::get_name() const
{
    return name;
}


int64_t NetcdfDimension::get_size() const
{
    return size;
}


bool NetcdfDimension::is_unlimited() const
{
    return unlimited;
}


int NetcdfDimension::get_id() const
{
    return id;
}


ostream& NetcdfDimension::print(ostream &os) const
{
    os << "NetcdfDimension(";
    os << name << ",";
    os << size << ",";
    os << unlimited << ")";
    return os;
}

