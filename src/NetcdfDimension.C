#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

#include "Common.H"
#include "Dataset.H"
#include "Mask.H"
#include "MaskMap.H"
#include "NetcdfDataset.H"
#include "NetcdfDimension.H"
#include "NetcdfError.H"
#include "Pnetcdf.H"
#include "Timing.H"


NetcdfDimension::NetcdfDimension(NetcdfDataset *dataset, int dimid)
    :   Dimension()
    ,   dataset(dataset)
    ,   id(dimid)
    ,   name("")
    ,   size(0)
    ,   unlimited(false)
{
    TIMING("NetcdfDimension::NetcdfDimension(NetcdfDataset*,int)");
    int ncid = dataset->get_id();
    int udim;
    ncmpi::inq_unlimdim(ncid, &udim);
    char name_tmp[MAX_NAME];
    MPI_Offset size_tmp;
    ncmpi::inq_dim(ncid, dimid, name_tmp, &size_tmp);
    name = string(name_tmp);
    size = size_tmp;
    unlimited = (udim == dimid);
}


NetcdfDimension::~NetcdfDimension()
{
    TIMING("NetcdfDimension::~NetcdfDimension()");
}


string NetcdfDimension::get_name() const
{
    TIMING("NetcdfDimension::get_name()");
    return name;
}


int64_t NetcdfDimension::get_size() const
{
    TIMING("NetcdfDimension::get_size()");
    if (get_dataset()->get_masks()) {
        return get_dataset()->get_masks()->get_mask(this)->get_count();
    }
    return size;
}


bool NetcdfDimension::is_unlimited() const
{
    TIMING("NetcdfDimension::is_unlimited()");
    return unlimited;
}


Dataset* NetcdfDimension::get_dataset() const
{
    TIMING("NetcdfDimension::get_dataset()");
    return dataset;
}


NetcdfDataset* NetcdfDimension::get_netcdf_dataset() const
{
    TIMING("NetcdfDimension::get_netcdf_dataset()");
    return dataset;
}


int NetcdfDimension::get_id() const
{
    TIMING("NetcdfDimension::get_id()");
    return id;
}


ostream& NetcdfDimension::print(ostream &os) const
{
    TIMING("NetcdfDimension::print(ostream)");
    os << "NetcdfDimension(";
    os << name << ",";
    os << size << ",";
    os << unlimited << ")";
    return os;
}
