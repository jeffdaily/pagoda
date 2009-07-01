#include <pnetcdf.h>

#include "Attribute.H"
#include "NetcdfAttribute.H"
#include "NetcdfDimension.H"
#include "NetcdfVariable.H"
#include "Util.H"


NetcdfVariable::NetcdfVariable(int ncid, int varid)
    :   Variable()
    ,   ncid(ncid)
    ,   id(varid)
    ,   name("")
    ,   dims()
    ,   atts()
    ,   type(DataType::CHAR)
{
    int ndim;
    int err = ncmpi_inq_ndims(ncid, &ndim);
    ERRNO_CHECK(err);
    char cname[MAX_NAME];
    int dim_ids[ndim]; // plenty big (file's ndim <= variable's ndim)
    nc_type type_tmp;
    int natt;
    err = ncmpi_inq_var(ncid, varid, cname, &type_tmp, &ndim, dim_ids, &natt);
    ERRNO_CHECK(err);
    name = string(cname);
    type = type_tmp;
    for (int dimid=0; dimid<ndim; ++dimid) {
        dims.push_back(new NetcdfDimension(ncid, dimid));
    }
    for (int attid=0; attid<natt; ++attid) {
        atts.push_back(new NetcdfAttribute(ncid, attid, varid));
    }
}


NetcdfVariable::NetcdfVariable(const NetcdfVariable &other)
    :   Variable(other)
    ,   ncid(other.ncid)
    ,   id(other.id)
    ,   name(other.name)
    ,   dims(other.dims)
    ,   atts(other.atts)
    ,   type(other.type)
{
}


NetcdfVariable::~NetcdfVariable()
{
}


NetcdfVariable& NetcdfVariable::operator = (const NetcdfVariable &other)
{
    ncid = other.ncid;
    id = other.id;
    name = other.name;
    dims = other.dims;
    atts = other.atts;
    type = other.type;
    return *this;
}


string NetcdfVariable::get_name() const
{
    return name;
}


bool NetcdfVariable::has_record() const
{
    return dims[0]->is_unlimited();
}


size_t NetcdfVariable::num_dims() const
{
    return dims.size();
}


vector<Dimension*> NetcdfVariable::get_dims() const
{
    return vector<Dimension*>(dims.begin(), dims.end());
}


size_t NetcdfVariable::num_atts() const
{
    return atts.size();
}


vector<Attribute*> NetcdfVariable::get_atts() const
{
    return vector<Attribute*>(atts.begin(), atts.end());
}


DataType NetcdfVariable::get_type() const
{
    return type;
}


ostream& NetcdfVariable::print(ostream &os) const
{
    os << "NetcdfVariable(" << name << ")";
    return os;
}

