#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <pnetcdf.h>

#include "Array.H"
#include "Hints.H"
#include "Grid.H"
#include "Pack.H"
#include "PnetcdfAttribute.H"
#include "PnetcdfDataset.H"
#include "PnetcdfDimension.H"
#include "PnetcdfError.H"
#include "PnetcdfVariable.H"
#include "PnetcdfNS.H"
#include "Util.H"


Dataset* pagoda_pnetcdf_open(const string &filename)
{
    Dataset *ret = NULL;
    try {
        ret = new PnetcdfDataset(filename);
    } catch (PagodaException &ex) {
        if (ret != NULL) {
            delete ret;
        }
        ret = NULL;
    }

    return ret;
}


DataType PnetcdfDataset::to_dt(const nc_type &type)
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


PnetcdfDataset::PnetcdfDataset(const string &filename)
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
    MPI_Info info = Hints::get_info();
    ncid = ncmpi::open(MPI_COMM_WORLD, filename, NC_NOWRITE, info);
    is_open = true;
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
    MPI_Info_free(&info);
}


PnetcdfDataset::~PnetcdfDataset()
{
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
    if (is_open) {
        is_open = false;
        ncmpi::close(ncid);
    }
}


vector<Attribute*> PnetcdfDataset::get_atts() const
{
    vector<Attribute*> ret;
    vector<PnetcdfAttribute*>::const_iterator it;


    for (it=atts.begin(); it!=atts.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


vector<Dimension*> PnetcdfDataset::get_dims() const
{
    vector<Dimension*> ret;
    vector<PnetcdfDimension*>::const_iterator it;


    for (it=dims.begin(); it!=dims.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


vector<Variable*> PnetcdfDataset::get_vars() const
{
    vector<Variable*> ret;
    vector<PnetcdfVariable*>::const_iterator it;


    for (it=vars.begin(); it!=vars.end(); ++it) {
        ret.push_back(*it);
    }

    return ret;
}


Attribute* PnetcdfDataset::get_att(const string &name,
                                   bool ignore_case,
                                   bool within) const
{
    return AbstractDataset::get_att(name, ignore_case, within);
}


Attribute* PnetcdfDataset::get_att(const vector<string> &names,
                                   bool ignore_case,
                                   bool within) const
{
    return AbstractDataset::get_att(names, ignore_case, within);
}


PnetcdfAttribute* PnetcdfDataset::get_att(size_t i) const
{
    return atts.at(i);
}


Dimension* PnetcdfDataset::get_dim(const string &name,
                                   bool ignore_case,
                                   bool within) const
{
    return AbstractDataset::get_dim(name, ignore_case, within);
}


PnetcdfDimension* PnetcdfDataset::get_dim(size_t i) const
{
    return dims.at(i);
}


Variable* PnetcdfDataset::get_var(const string &name,
                                  bool ignore_case,
                                  bool within) const
{
    return AbstractDataset::get_var(name, ignore_case, within);
}


PnetcdfVariable* PnetcdfDataset::get_var(size_t i) const
{
    return vars.at(i);
}


void PnetcdfDataset::wait()
{
    vector<int> requests;
    vector<PnetcdfVariable*>::iterator var_it;
    vector<PnetcdfVariable*>::iterator var_end = vars.end();


    // gather requests from all Variables
    for (var_it=vars.begin(); var_it!=var_end; ++var_it) {
        PnetcdfVariable *var = *var_it;
        requests.insert(requests.end(),
                var->nb_requests.begin(), var->nb_requests.end());
    }
    if (!requests.empty()) {
        vector<int> statuses(requests.size());
        ncmpi::wait_all(ncid, requests, statuses);
        requests.clear();
        // now post process the Variables
        for (var_it=vars.begin(); var_it!=var_end; ++var_it) {
            PnetcdfVariable *var = *var_it;
            var->after_wait();
        }
    }
}


FileFormat PnetcdfDataset::get_file_format() const
{
    int format = ncmpi::inq_format(ncid);
    if (format == NC_FORMAT_64BIT_DATA) {
        return FF_CDF5;
    }
    else if (format == NC_FORMAT_64BIT) {
        return FF_CDF2;
    }
    else if (format == NC_FORMAT_CLASSIC) {
        return FF_CDF1;
    }
    else {
        ERR("unknown file format");
    }
    //return FF_UNKNOWN; // unreachable
}


ostream& PnetcdfDataset::print(ostream &os) const
{
    return os << "PnetcdfDataset(" << filename << ")";
}


string PnetcdfDataset::get_filename() const
{
    return filename;
}


int PnetcdfDataset::get_id() const
{
    return ncid;
}
