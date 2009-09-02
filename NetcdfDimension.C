#include <pnetcdf.h>

#include "Common.H"
#include "NetcdfDataset.H"
#include "NetcdfDimension.H"
#include "NetcdfError.H"


NetcdfDimension::NetcdfDimension(NetcdfDataset *dataset, int dimid)
    :   Dimension()
    ,   dataset(dataset)
    ,   id(dimid)
    ,   name("")
    ,   size(0)
    ,   unlimited(false)
{
    int ncid = dataset->get_id();
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


NetcdfDimension::~NetcdfDimension()
{
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


NetcdfDataset* NetcdfDimension::get_dataset() const
{
    return dataset;
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

