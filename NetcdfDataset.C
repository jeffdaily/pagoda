#include <pnetcdf.h>

#include "NetcdfAttribute.H"
#include "NetcdfDataset.H"
#include "NetcdfDimension.H"
#include "NetcdfVariable.H"
#include "Util.H"


NetcdfDataset::NetcdfDataset(const string &filename)
    :   Dataset()
    ,   filename(filename)
    ,   ncid(-1)
    ,   udim(-1)
    ,   atts()
    ,   dims()
    ,   vars()
{
    int err;
    int ndim;
    int nvar;
    int natt;
    err = ncmpi_open(MPI_COMM_WORLD, filename.c_str(), NC_NOWRITE,
            MPI_INFO_NULL, &ncid);
    ERRNO_CHECK(err);
    err = ncmpi_inq(ncid, &ndim, &nvar, &natt, &udim);
    ERRNO_CHECK(err);
    for (int attid=0; attid<natt; ++attid) {
        atts.push_back(new NetcdfAttribute(ncid, attid, NC_GLOBAL));
    }
    for (int dimid=0; dimid<ndim; ++dimid) {
        dims.push_back(new NetcdfDimension(ncid, dimid));
    }
    for (int varid=0; varid<nvar; ++varid) {
        vars.push_back(new NetcdfVariable(ncid, varid));
    }
}


NetcdfDataset::~NetcdfDataset()
{
    using Util::ptr_deleter;
    transform(atts.begin(), atts.end(), atts.begin(), ptr_deleter<Attribute*>);
    transform(dims.begin(), dims.end(), dims.begin(), ptr_deleter<Dimension*>);
    transform(vars.begin(), vars.end(), vars.begin(), ptr_deleter<Variable*>);
    ERRNO_CHECK(ncmpi_close(ncid));
}


vector<Attribute*>& NetcdfDataset::get_atts()
{
    return atts;
}


vector<Dimension*>& NetcdfDataset::get_dims()
{
    return dims;
}


vector<Variable*>& NetcdfDataset::get_vars()
{
    return vars;
}


ostream& NetcdfDataset::print(ostream &os) const
{
    return os << "NetcdfDataset(" << filename << ")";
}

