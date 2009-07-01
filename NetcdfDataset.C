#include <pnetcdf.h>

#include "NetcdfAttribute.H"
#include "NetcdfDataset.H"
#include "NetcdfDimension.H"
#include "NetcdfVariable.H"
#include "Util.H"


NetcdfDataset::NetcdfDataset(const string &filename)
    :   Dataset()
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
    for (int attid=0; attid<ndim; ++attid) {
        atts.push_back(new NetcdfAttribute(ncid, attid, NC_GLOBAL));
    }
    for (int dimid=0; dimid<ndim; ++dimid) {
        dims.push_back(new NetcdfDimension(ncid, dimid));
    }
    for (int varid=0; varid<nvar; ++varid) {
        vars.push_back(new NetcdfVariable(ncid, varid));
    }
}


vector<Attribute*> NetcdfDataset::get_atts()
{
    return vector<Attribute*>(atts.begin(), atts.end());
}


vector<Dimension*> NetcdfDataset::get_dims()
{
    return vector<Dimension*>(dims.begin(), dims.end());
}


vector<Variable*> NetcdfDataset::get_vars()
{
    return vector<Variable*>(vars.begin(), vars.end());
}


ostream& NetcdfDataset::print(ostream &os) const
{
    return os;
}

