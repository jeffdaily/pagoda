#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <pnetcdf.h>

#include "Common.H"
#include "Dataset.H"
#include "Mask.H"
#include "MaskMap.H"
#include "PnetcdfDataset.H"
#include "PnetcdfDimension.H"
#include "PnetcdfError.H"
#include "PnetcdfNS.H"
#include "Timing.H"


PnetcdfDimension::PnetcdfDimension(PnetcdfDataset *dataset, int dimid)
    :   Dimension()
    ,   dataset(dataset)
    ,   id(dimid)
    ,   name("")
    ,   size(0)
    ,   unlimited(false)
{
    int ncid = dataset->get_id();
    int udim = ncmpi::inq_unlimdim(ncid);
    MPI_Offset size_tmp;
    ncmpi::inq_dim(ncid, dimid, name, size_tmp);
    size = size_tmp;
    unlimited = (udim == dimid);
}


PnetcdfDimension::~PnetcdfDimension()
{
}


string PnetcdfDimension::get_name() const
{
    return name;
}


int64_t PnetcdfDimension::get_size() const
{
    Mask *mask = NULL;
    if ((mask = get_mask())) {
        return mask->get_count();
    }
    return size;
}


bool PnetcdfDimension::is_unlimited() const
{
    return unlimited;
}


Dataset* PnetcdfDimension::get_dataset() const
{
    return dataset;
}


PnetcdfDataset* PnetcdfDimension::get_netcdf_dataset() const
{
    return dataset;
}


int PnetcdfDimension::get_id() const
{
    return id;
}


ostream& PnetcdfDimension::print(ostream &os) const
{
    os << "PnetcdfDimension(";
    os << name << ",";
    os << size << ",";
    os << unlimited << ")";
    return os;
}
