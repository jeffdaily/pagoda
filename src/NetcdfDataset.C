#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

#include "Array.H"
#include "Grid.H"
#include "NetcdfAttribute.H"
#include "NetcdfDataset.H"
#include "NetcdfDimension.H"
#include "NetcdfError.H"
#include "NetcdfVariable.H"
#include "Pnetcdf.H"
#include "Timing.H"
#include "Util.H"


NetcdfDataset::NetcdfDataset(const string &filename)
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
    TIMING("NetcdfDataset::NetcdfDataset(string)");
    int ndim;
    int nvar;
    int natt;
    ncmpi::open(MPI_COMM_WORLD, filename.c_str(), NC_NOWRITE, MPI_INFO_NULL, &ncid);
    ncmpi::inq(ncid, &ndim, &nvar, &natt, &udim);
    for (int attid=0; attid<natt; ++attid) {
        atts.push_back(new NetcdfAttribute(this, attid));
    }
    for (int dimid=0; dimid<ndim; ++dimid) {
        dims.push_back(new NetcdfDimension(this, dimid));
    }
    for (int varid=0; varid<nvar; ++varid) {
        vars.push_back(new NetcdfVariable(this, varid));
    }
}


NetcdfDataset::~NetcdfDataset()
{
    TIMING("NetcdfDataset::~NetcdfDataset()");
    using pagoda::ptr_deleter;
    transform(atts.begin(), atts.end(), atts.begin(),
            ptr_deleter<NetcdfAttribute*>);
    transform(dims.begin(), dims.end(), dims.begin(),
            ptr_deleter<NetcdfDimension*>);
    transform(vars.begin(), vars.end(), vars.begin(),
            ptr_deleter<NetcdfVariable*>);
    close();
}


void NetcdfDataset::close()
{
    if (open) {
        open = false;
        ncmpi::close(ncid);
    }
}


vector<Attribute*> NetcdfDataset::get_atts() const
{
    vector<Attribute*> ret;
    vector<NetcdfAttribute*>::const_iterator it;

    TIMING("NetcdfDataset::get_atts()");

    for (it=atts.begin(); it!=atts.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


vector<Dimension*> NetcdfDataset::get_dims() const
{
    vector<Dimension*> ret;
    vector<NetcdfDimension*>::const_iterator it;

    TIMING("NetcdfDataset::get_dims()");

    for (it=dims.begin(); it!=dims.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


vector<Variable*> NetcdfDataset::get_vars() const
{
    vector<Variable*> ret;
    vector<NetcdfVariable*>::const_iterator it;

    TIMING("NetcdfDataset::get_vars()");

    for (it=vars.begin(); it!=vars.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


NetcdfAttribute* NetcdfDataset::get_att(size_t i) const
{
    TIMING("NetcdfDataset::get_att(size_t)");
    return atts.at(i);
}


NetcdfDimension* NetcdfDataset::get_dim(size_t i) const
{
    TIMING("NetcdfDataset::get_dim(size_t)");
    return dims.at(i);
}


NetcdfVariable* NetcdfDataset::get_var(size_t i) const
{
    TIMING("NetcdfDataset::get_var(size_t)");
    return vars.at(i);
}


void NetcdfDataset::wait()
{
    TIMING("NetcdfDataset::wait()");

    if (!requests.empty()) {
        vector<int> statuses(requests.size());
        ncmpi::wait_all(ncid, requests.size(), &requests[0], &statuses[0]);
        for (size_t i=0; i<arrays_to_release.size(); ++i) {
            arrays_to_release[i]->release_update();
        }
        arrays_to_release.clear();
        requests.clear();
    }
}


ostream& NetcdfDataset::print(ostream &os) const
{
    TIMING("NetcdfDataset::print(ostream)");
    return os << "NetcdfDataset(" << filename << ")";
}


string NetcdfDataset::get_filename() const
{
    TIMING("NetcdfDataset::get_filename()");
    return filename;
}


int NetcdfDataset::get_id() const
{
    TIMING("NetcdfDataset::get_id()");
    return ncid;
}


NetcdfDimension* NetcdfDataset::get_udim() const
{
    TIMING("NetcdfDataset::get_udim()");
    return dims.at(udim);
}
