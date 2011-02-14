#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <netcdf.h>

#include "Array.H"
#include "Grid.H"
#include "Pack.H"
#include "Netcdf4Attribute.H"
#include "Netcdf4Dataset.H"
#include "Netcdf4Dimension.H"
#include "Netcdf4Error.H"
#include "Netcdf4Variable.H"
#include "Netcdf4.H"
#include "Timing.H"
#include "Util.H"


Dataset* pagoda_netcdf4_open(const string &filename)
{
    Dataset *ret = NULL;
    try {
        ret = new Netcdf4Dataset(filename);
    } catch (PagodaException &ex) {
        if (ret != NULL) {
            delete ret;
        }
        ret = NULL;
    }

    return ret;
}


/**@todo TODO fix Netcdf4Dataset::to_dt */
DataType Netcdf4Dataset::to_dt(const nc_type &type)
{
    if (NC_NAT == type) {        // Not A Type
        throw DataTypeException("NC_NAT not supported");
    }
    else if (NC_BYTE == type) {   // signed 1 byte integer
        return DataType::SCHAR;
    }
    else if (NC_CHAR == type) {   // ISO/ASCII character
        return DataType::CHAR;
    }
    else if (NC_SHORT == type) {   // signed 2 byte integer
#if   2 == SIZEOF_SHORT
        return DataType::SHORT;
#elif 2 == SIZEOF_INT
        return DataType::INT;
#elif 2 == SIZEOF_LONG
        return DataType::LONG;
#elif 2 == SIZEOF_LONG_LONG
        return DataType::LONGLONG;
#else
        throw DataTypeException("no corresponding C type for NC_SHORT");
#endif
    }
    else if (NC_INT == type) {   // signed 4 byte integer
#if   4 == SIZEOF_SHORT
        return DataType::SHORT;
#elif 4 == SIZEOF_INT
        return DataType::INT;
#elif 4 == SIZEOF_LONG
        return DataType::LONG;
#elif 4 == SIZEOF_LONG_LONG
        return DataType::LONLONG;
#else
        throw DataTypeException("no corresponding C type for NC_INT");
#endif
    }
    else if (NC_FLOAT == type) {   // single precision floating point number
#if   4 == SIZEOF_FLOAT
        return DataType::FLOAT;
#elif 4 == SIZEOF_DOUBLE
        return DataType::DOUBLE;
#elif 4 == SIZEOF_LONG_DOUBLE
        return DOUBLE::LONGDOUBLE;
#else
        throw DataTypeException("no corresponding C type for NC_FLOAT");
#endif
    }
    else if (NC_DOUBLE == type) {   // double precision floating point number
#if   8 == SIZEOF_FLOAT
        return DataType::FLOAT;
#elif 8 == SIZEOF_DOUBLE
        return DataType::DOUBLE;
#elif 8 == SIZEOF_LONG_DOUBLE
        return DOUBLE::LONGDOUBLE;
#else
        throw DataTypeException("no corresponding C type for NC_DOUBLE");
#endif
    }
    throw DataTypeException("could not determine DataType from nc_type");
}


Netcdf4Dataset::Netcdf4Dataset(const string &filename)
    :   AbstractDataset()
    ,   filename(filename)
    ,   ncid(-1)
    ,   udim(-1)
    ,   atts()
    ,   dims()
    ,   vars()
    ,   is_open(false)
{
    int ndim;
    int nvar;
    int natt;
    ncid = nc::open(filename, NC_NOWRITE, MPI_COMM_WORLD, MPI_INFO_NULL);
    is_open = true;
    nc::inq(ncid, ndim, nvar, natt, udim);
    for (int attid=0; attid<natt; ++attid) {
        atts.push_back(new Netcdf4Attribute(this, attid));
    }
    for (int dimid=0; dimid<ndim; ++dimid) {
        dims.push_back(new Netcdf4Dimension(this, dimid));
    }
    for (int varid=0; varid<nvar; ++varid) {
        vars.push_back(new Netcdf4Variable(this, varid));
    }
}


Netcdf4Dataset::~Netcdf4Dataset()
{
    using pagoda::ptr_deleter;
    transform(atts.begin(), atts.end(), atts.begin(),
              ptr_deleter<Netcdf4Attribute*>);
    transform(dims.begin(), dims.end(), dims.begin(),
              ptr_deleter<Netcdf4Dimension*>);
    transform(vars.begin(), vars.end(), vars.begin(),
              ptr_deleter<Netcdf4Variable*>);
    close();
}


void Netcdf4Dataset::close()
{
    if (is_open) {
        is_open = false;
        nc::close(ncid);
    }
}


vector<Attribute*> Netcdf4Dataset::get_atts() const
{
    vector<Attribute*> ret;
    vector<Netcdf4Attribute*>::const_iterator it;


    for (it=atts.begin(); it!=atts.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


vector<Dimension*> Netcdf4Dataset::get_dims() const
{
    vector<Dimension*> ret;
    vector<Netcdf4Dimension*>::const_iterator it;


    for (it=dims.begin(); it!=dims.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


vector<Variable*> Netcdf4Dataset::get_vars() const
{
    vector<Variable*> ret;
    vector<Netcdf4Variable*>::const_iterator it;


    for (it=vars.begin(); it!=vars.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


Attribute* Netcdf4Dataset::get_att(const string &name,
                                   bool ignore_case,
                                   bool within) const
{
    return AbstractDataset::get_att(name, ignore_case, within);
}


Attribute* Netcdf4Dataset::get_att(const vector<string> &names,
                                   bool ignore_case,
                                   bool within) const
{
    return AbstractDataset::get_att(names, ignore_case, within);
}


Netcdf4Attribute* Netcdf4Dataset::get_att(size_t i) const
{
    return atts.at(i);
}


Dimension* Netcdf4Dataset::get_dim(const string &name,
                                   bool ignore_case,
                                   bool within) const
{
    return AbstractDataset::get_dim(name, ignore_case, within);
}


Netcdf4Dimension* Netcdf4Dataset::get_dim(size_t i) const
{
    return dims.at(i);
}


Variable* Netcdf4Dataset::get_var(const string &name,
                                  bool ignore_case,
                                  bool within) const
{
    return AbstractDataset::get_var(name, ignore_case, within);
}


Netcdf4Variable* Netcdf4Dataset::get_var(size_t i) const
{
    return vars.at(i);
}


void Netcdf4Dataset::wait()
{
}


/**@todo TODO fix Netcdf4Dataset::get_file_format */
FileFormat Netcdf4Dataset::get_file_format() const
{
    int format = nc::inq_format(ncid);
    if (format == NC_FORMAT_CLASSIC) {
        return FF_NETCDF3_CDF1;
    }
    else if (format == NC_FORMAT_64BIT) {
        return FF_NETCDF3_CDF2;
    }
    else if (format == NC_FORMAT_NETCDF4) {
        return FF_NETCDF4;
    }
    else if (format == NC_FORMAT_NETCDF4_CLASSIC) {
        return FF_NETCDF4_CLASSIC;
    }
    else {
        ERR("unknown file format");
    }
    //return FF_UNKNOWN; // unreachable
}


ostream& Netcdf4Dataset::print(ostream &os) const
{
    return os << "Netcdf4Dataset(" << filename << ")";
}


string Netcdf4Dataset::get_filename() const
{
    return filename;
}


int Netcdf4Dataset::get_id() const
{
    return ncid;
}
