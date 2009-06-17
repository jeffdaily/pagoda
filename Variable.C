#include <pnetcdf.h>

#include "Variable.H"
#include "Util.H"


Variable::Variable()
    :   name("")
    ,   dimensions()
    ,   attributes()
    ,   file("")
    ,   type(DataType::CHAR)
{
}


Variable::Variable(int ncid, int varid, string file)
    :   name("")
    ,   dimensions()
    ,   attributes()
    ,   file(file)
    ,   type(DataType::CHAR)
{
    int ndim;
    int err = ncmpi_inq_ndims(ncid, &ndim);
    ERRNO_CHECK(err);
    char cname[MAX_NAME];
    int dims[ndim]; // plenty big
    nc_type type_tmp;
    int natt;
    err = ncmpi_inq_var(ncid, varid, cname, &type_tmp, &ndim, dims, &natt);
    ERRNO_CHECK(err);
    name = string(cname);
    type = type_tmp;
    for (int dimid=0; dimid<ndim; ++dimid) {
        Dimension dim(ncid, dimid);
        dimensions.push_back(dim);
    }
    for (int attid=0; attid<natt; ++attid) {
        Attribute att(ncid, attid, varid, file);
        attributes.push_back(att);
    }
}


Variable::Variable(const Variable &copy)
    :   name(copy.name)
    ,   dimensions(copy.dimensions)
    ,   attributes(copy.attributes)
    ,   file(copy.file)
    ,   type(copy.type)
{
}


Variable::~Variable()
{
}


Variable& Variable::operator = (const Variable &other)
{
    name = other.name;
    dimensions = other.dimensions;
    attributes = other.attributes;
    file = other.file;
    type = other.type;
    return *this;
}


bool Variable::operator == (const Variable &other) const
{
    return name == other.name
            && dimensions == other.dimensions
            && attributes == other.attributes;
}


string Variable::get_name() const
{
    return name;
}


bool Variable::has_record() const
{
    return dimensions[0].is_unlimited();
}


size_t Variable::num_dimensions() const
{
    return dimensions.size();
}


vector<Dimension> Variable::get_dimensions() const
{
    return dimensions;
}


size_t Variable::num_attributes() const
{
    return attributes.size();
}


vector<Attribute> Variable::get_attributes() const
{
    return attributes;
}


DataType Variable::get_type() const
{
    return type;
}


ostream& operator << (ostream &os, const Variable &other)
{
    os << "Variable(";
    os << other.name << ")";
    return os;
}

