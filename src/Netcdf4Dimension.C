#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <netcdf.h>

#include "Common.H"
#include "Dataset.H"
#include "MaskMap.H"
#include "Netcdf4Dataset.H"
#include "Netcdf4Dimension.H"
#include "Netcdf4Error.H"
#include "Netcdf4.H"


Netcdf4Dimension::Netcdf4Dimension(Netcdf4Dataset *dataset, int dimid)
    :   Dimension()
    ,   dataset(dataset)
    ,   id(dimid)
    ,   name("")
    ,   size(0)
    ,   unlimited(false)
{
    int ncid = dataset->get_id();
    int udim = nc::inq_unlimdim(ncid);
    size_t size_tmp;
    nc::inq_dim(ncid, dimid, name, size_tmp);
    size = size_tmp;
    unlimited = (udim == dimid);
}


Netcdf4Dimension::~Netcdf4Dimension()
{
}


string Netcdf4Dimension::get_name() const
{
    return name;
}


int64_t Netcdf4Dimension::get_size() const
{
    Array *mask = NULL;
    if ((mask = get_mask())) {
        return mask->get_count();
    }
    return size;
}


bool Netcdf4Dimension::is_unlimited() const
{
    return unlimited;
}


Dataset* Netcdf4Dimension::get_dataset() const
{
    return dataset;
}


Netcdf4Dataset* Netcdf4Dimension::get_netcdf_dataset() const
{
    return dataset;
}


int Netcdf4Dimension::get_id() const
{
    return id;
}


ostream& Netcdf4Dimension::print(ostream &os) const
{
    os << "Netcdf4Dimension(";
    os << name << ",";
    os << size << ",";
    os << unlimited << ")";
    return os;
}
