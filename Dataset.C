#include <iostream>
    using std::endl;

#include <mpi.h>
#include <pnetcdf.h>

#include "Dataset.H"
#include "Util.H"

static int err;


Dataset::Dataset()
    :   name("")
    ,   attributes()
    ,   dimensions()
    ,   variables()
{
}


Dataset::Dataset(const Dataset &copy)
    :   name(copy.name)
    ,   attributes(copy.attributes)
    ,   dimensions(copy.dimensions)
    ,   variables(copy.variables)
{
}


Dataset::~Dataset()
{
}


void Dataset::add_file(string file)
{
    int id;
    err = ncmpi_open(MPI_COMM_WORLD, file.c_str(), NC_NOWRITE,
            MPI_INFO_NULL, &id);
    ERRNO_CHECK(err);

    int ndims, nvars, natts, unlimited_dim;
    err = ncmpi_inq(id, &ndims, &nvars, &natts, &unlimited_dim);
    ERRNO_CHECK(err);

    for (int dimid=0; dimid<ndims; ++dimid) {
        Dimension new_dim(id, dimid);
        if (! has_dimension(new_dim)) {
            dimensions.push_back(new_dim);
        }
    }

    for (int varid=0; varid<nvars; ++varid) {
        Variable new_var(id, varid);
        if (! has_variable(new_var)) {
            variables.push_back(new_var);
        }
    }

    for (int attid=0; attid<natts; ++attid) {
        Attribute new_att(id, attid);
        if (! has_attribute(new_att)) {
            attributes.push_back(new_att);
        }
    }

    err = ncmpi_close(id);
    ERRNO_CHECK(err);
}


ostream& operator << (ostream &os, const Dataset &other)
{
    string S = "        ";
    os << "netcdf " << other.name << " {" << endl;
    os << "dimensions:" << endl;
    for (size_t i=0,limit=other.dimensions.size(); i<limit; ++i) {
        Dimension dim = other.dimensions[i];
        os << S << dim.get_name() << " = ";
        if (dim.is_unlimited()) {
            os << "UNLIMITED ; // (" << dim.get_size() << " currently)" << endl;
        } else {
            os << dim.get_size() << " ;" << endl;
        }
    }
    os << "variables:" << endl;
    for (size_t i=0,limit=other.variables.size(); i<limit; ++i) {
        Variable var = other.variables[i];
        os << S << var.get_type() << " " << var.get_name() << "(";
        vector<Dimension> dims = var.get_dimensions();
        for (size_t j=0,limit=dims.size(); j<limit; ++j) {
            Dimension dim = dims[j];
            os << dim.get_name();
            if (j+1 != limit) os << ", ";
        }
        os << ") ;" << endl;
        vector<Attribute> atts = var.get_attributes();
        for (size_t j=0,limit=atts.size(); j<limit; ++j) {
            os << S << S << atts[j] << endl;
        }
    }
    os << "// global attributes:" << endl;
    for (size_t i=0,limit=other.attributes.size(); i<limit; ++i) {
        os << other.attributes[i] << endl;
    }
    return os;
}


/**
 * See if this Dataset already has the given dimension.
 *
 * The name is the primary criteria for a match. However, if the count
 * or unlimited properties do not match, this indicates a problem with
 * the Dataset.
 */
bool Dataset::has_dimension(Dimension dimension) const
{
    string name = dimension.get_name();
    for (size_t i=0,limit=dimensions.size(); i<limit; ++i) {
        Dimension current = dimensions[i];
        if (name == current.get_name()) {
            assert(dimension.get_size() == current.get_size()
                    && dimension.is_unlimited() == current.is_unlimited());
            return true;
        }
    }
    return false;
}


/**
 * See if this Dataset already has the given variable.
 *
 * The name is the primary criteria for a match.
 */
bool Dataset::has_variable(Variable variable) const
{
    string name = variable.get_name();
    for (size_t i=0,limit=variables.size(); i<limit; ++i) {
        Variable current = variables[i];
        if (name == current.get_name()) {
            return true;
        }
    }
    return false;
}


/**
 * See if this Dataset already has the given attribute.
 *
 * The name is the primary criteria for a match.
 */
bool Dataset::has_attribute(Attribute attribute) const
{
    string name = attribute.get_name();
    for (size_t i=0,limit=attributes.size(); i<limit; ++i) {
        Attribute current = attributes[i];
        if (name == current.get_name()) {
            return true;
        }
    }
    return false;
}

