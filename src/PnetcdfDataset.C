#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

#include "Array.H"
#include "Grid.H"
#include "PnetcdfAttribute.H"
#include "PnetcdfDataset.H"
#include "PnetcdfDimension.H"
#include "PnetcdfError.H"
#include "PnetcdfVariable.H"
#include "Pnetcdf.H"
#include "Timing.H"
#include "Util.H"


PnetcdfDataset::PnetcdfDataset(const string &filename)
    :   AbstractDataset()
    ,   filename(filename)
    ,   ncid(-1)
    ,   udim(-1)
    ,   atts()
    ,   dims()
    ,   vars()
    ,   requests()
    ,   arrays_to_release()
    ,   open(true)
{
    TIMING("PnetcdfDataset::PnetcdfDataset(string)");
    int ndim;
    int nvar;
    int natt;
    ncid = ncmpi::open(MPI_COMM_WORLD, filename, NC_NOWRITE, MPI_INFO_NULL);
    ncmpi::inq(ncid, ndim, nvar, natt, udim);
    for (int attid=0; attid<natt; ++attid) {
        atts.push_back(new PnetcdfAttribute(this, attid));
    }
    for (int dimid=0; dimid<ndim; ++dimid) {
        dims.push_back(new PnetcdfDimension(this, dimid));
    }
    for (int varid=0; varid<nvar; ++varid) {
        vars.push_back(new PnetcdfVariable(this, varid));
    }
}


PnetcdfDataset::~PnetcdfDataset()
{
    TIMING("PnetcdfDataset::~PnetcdfDataset()");
    using pagoda::ptr_deleter;
    transform(atts.begin(), atts.end(), atts.begin(),
            ptr_deleter<PnetcdfAttribute*>);
    transform(dims.begin(), dims.end(), dims.begin(),
            ptr_deleter<PnetcdfDimension*>);
    transform(vars.begin(), vars.end(), vars.begin(),
            ptr_deleter<PnetcdfVariable*>);
    close();
}


void PnetcdfDataset::close()
{
    if (open) {
        open = false;
        ncmpi::close(ncid);
    }
}


vector<Attribute*> PnetcdfDataset::get_atts() const
{
    vector<Attribute*> ret;
    vector<PnetcdfAttribute*>::const_iterator it;

    TIMING("PnetcdfDataset::get_atts()");

    for (it=atts.begin(); it!=atts.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


vector<Dimension*> PnetcdfDataset::get_dims() const
{
    vector<Dimension*> ret;
    vector<PnetcdfDimension*>::const_iterator it;

    TIMING("PnetcdfDataset::get_dims()");

    for (it=dims.begin(); it!=dims.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


vector<Variable*> PnetcdfDataset::get_vars() const
{
    vector<Variable*> ret;
    vector<PnetcdfVariable*>::const_iterator it;

    TIMING("PnetcdfDataset::get_vars()");

    for (it=vars.begin(); it!=vars.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


PnetcdfAttribute* PnetcdfDataset::get_att(size_t i) const
{
    TIMING("PnetcdfDataset::get_att(size_t)");
    return atts.at(i);
}


PnetcdfDimension* PnetcdfDataset::get_dim(size_t i) const
{
    TIMING("PnetcdfDataset::get_dim(size_t)");
    return dims.at(i);
}


PnetcdfVariable* PnetcdfDataset::get_var(size_t i) const
{
    TIMING("PnetcdfDataset::get_var(size_t)");
    return vars.at(i);
}


void PnetcdfDataset::wait()
{
    TIMING("PnetcdfDataset::wait()");

    if (!requests.empty()) {
        vector<int> statuses(requests.size());
        ncmpi::wait_all(ncid, requests, statuses);
        for (size_t i=0; i<arrays_to_release.size(); ++i) {
            arrays_to_release[i]->release_update();
        }
        arrays_to_release.clear();
        requests.clear();
    }
}


FileFormat PnetcdfDataset::get_file_format() const
{
    int format = ncmpi::inq_format(ncid);
    if (format == NC_FORMAT_64BIT_DATA) {
        return FF_PNETCDF_CDF5;
    } else if (format == NC_FORMAT_64BIT) {
        return FF_PNETCDF_CDF2;
    } else if (format == NC_FORMAT_CLASSIC) {
        return FF_PNETCDF_CDF1;
    } else {
        ERR("unknown file format");
    }
    return FF_UNKNOWN;
}


ostream& PnetcdfDataset::print(ostream &os) const
{
    TIMING("PnetcdfDataset::print(ostream)");
    return os << "PnetcdfDataset(" << filename << ")";
}


string PnetcdfDataset::get_filename() const
{
    TIMING("PnetcdfDataset::get_filename()");
    return filename;
}


int PnetcdfDataset::get_id() const
{
    TIMING("PnetcdfDataset::get_id()");
    return ncid;
}


PnetcdfDimension* PnetcdfDataset::get_udim() const
{
    TIMING("PnetcdfDataset::get_udim()");
    return dims.at(udim);
}
