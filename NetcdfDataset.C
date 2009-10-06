#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

#include "NetcdfAttribute.H"
#include "NetcdfDataset.H"
#include "NetcdfDimension.H"
#include "NetcdfError.H"
#include "NetcdfVariable.H"
#include "PnetcdfTiming.H"
#include "Timing.H"
#include "Util.H"


NetcdfDataset::NetcdfDataset(const string &filename)
    :   Dataset()
    ,   filename(filename)
    ,   ncid(-1)
    ,   udim(-1)
    ,   atts()
    ,   dims()
    ,   vars()
    ,   decorated()
{
    TIMING("NetcdfDataset::NetcdfDataset(string)");
    int err;
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
    using Util::ptr_deleter;
    transform(atts.begin(), atts.end(), atts.begin(),
            ptr_deleter<NetcdfAttribute*>);
    transform(dims.begin(), dims.end(), dims.begin(),
            ptr_deleter<NetcdfDimension*>);
    transform(vars.begin(), vars.end(), vars.begin(),
            ptr_deleter<NetcdfVariable*>);
    ncmpi::close(ncid);
}


vector<Attribute*> NetcdfDataset::get_atts()
{
    TIMING("NetcdfDataset::get_atts()");
    vector<Attribute*> ret;
    vector<NetcdfAttribute*>::iterator it;
    for (it=atts.begin(); it!=atts.end(); ++it) {
        ret.push_back(*it);
    }
    return ret;
}


vector<Dimension*> NetcdfDataset::get_dims()
{
    TIMING("NetcdfDataset::get_dims()");
    vector<Dimension*> ret;
    vector<NetcdfDimension*>::iterator it;
    for (it=dims.begin(); it!=dims.end(); ++it) {
        ret.push_back(*it);
    }
    return ret;
}


vector<Variable*> NetcdfDataset::get_vars()
{
    TIMING("NetcdfDataset::get_vars()");
    if (decorated.empty()) {
        vector<Variable*> ret;
        vector<NetcdfVariable*>::iterator it;
        for (it=vars.begin(); it!=vars.end(); ++it) {
            ret.push_back(*it);
        }
        return ret;
    } else {
        return decorated;
    }
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


void NetcdfDataset::decorate_set(const vector<Variable*> &vars)
{
    TIMING("NetcdfDataset::decorate_set(vector<Variable*>)");
    decorated = vars;
}
