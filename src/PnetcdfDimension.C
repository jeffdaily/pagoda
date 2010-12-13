#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

#include "Common.H"
#include "Dataset.H"
#include "Mask.H"
#include "MaskMap.H"
#include "PnetcdfDataset.H"
#include "PnetcdfDimension.H"
#include "PnetcdfError.H"
#include "Pnetcdf.H"
#include "Timing.H"


PnetcdfDimension::PnetcdfDimension(PnetcdfDataset *dataset, int dimid)
    :   Dimension()
    ,   dataset(dataset)
    ,   id(dimid)
    ,   name("")
    ,   size(0)
    ,   unlimited(false)
{
    TIMING("PnetcdfDimension::PnetcdfDimension(PnetcdfDataset*,int)");
    int ncid = dataset->get_id();
    int udim = ncmpi::inq_unlimdim(ncid);
    MPI_Offset size_tmp;
    ncmpi::inq_dim(ncid, dimid, name, size_tmp);
    size = size_tmp;
    unlimited = (udim == dimid);
}


PnetcdfDimension::~PnetcdfDimension()
{
    TIMING("PnetcdfDimension::~PnetcdfDimension()");
}


string PnetcdfDimension::get_name() const
{
    TIMING("PnetcdfDimension::get_name()");
    return name;
}


int64_t PnetcdfDimension::get_size() const
{
    Mask *mask = NULL;
    TIMING("PnetcdfDimension::get_size()");
    if ((mask = get_mask())) {
        return mask->get_count();
    }
    return size;
}


bool PnetcdfDimension::is_unlimited() const
{
    TIMING("PnetcdfDimension::is_unlimited()");
    return unlimited;
}


Dataset* PnetcdfDimension::get_dataset() const
{
    TIMING("PnetcdfDimension::get_dataset()");
    return dataset;
}


PnetcdfDataset* PnetcdfDimension::get_netcdf_dataset() const
{
    TIMING("PnetcdfDimension::get_netcdf_dataset()");
    return dataset;
}


int PnetcdfDimension::get_id() const
{
    TIMING("PnetcdfDimension::get_id()");
    return id;
}


ostream& PnetcdfDimension::print(ostream &os) const
{
    TIMING("PnetcdfDimension::print(ostream)");
    os << "PnetcdfDimension(";
    os << name << ",";
    os << size << ",";
    os << unlimited << ")";
    return os;
}
