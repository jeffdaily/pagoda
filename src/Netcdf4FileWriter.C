#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cassert>
#include <exception>
#include <string>
#include <vector>

#include <netcdf.h>

#include "Attribute.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "Mask.H"
#include "Netcdf4Dimension.H"
#include "Netcdf4Error.H"
#include "Netcdf4FileWriter.H"
#include "Netcdf4Variable.H"
#include "Pack.H"
#include "Netcdf4.H"
#include "Util.H"
#include "Timing.H"
#include "Values.H"

using std::invalid_argument;
using std::string;
using std::vector;


FileWriter* pagoda_netcdf4_create(const string &filename)
{
    FileWriter *ret = NULL;

    try {
        ret = new Netcdf4FileWriter(filename);
    } catch (PagodaException &ex) {
        if (ret != NULL) {
            delete ret;
        }
        ret = NULL;
    }

    return ret;
}


static int file_format_to_nc_format(FileFormat format)
{
    assert(FF_NETCDF4 <= format && format <= FF_NETCDF4_CLASSIC);
    if (format == FF_NETCDF4) {
        return NC_NETCDF4;
    }
    else if (format == FF_NETCDF4_CLASSIC) {
        return NC_FORMAT_CLASSIC|NC_NETCDF4;
    }
    else {
        ERR("FileFormat not recognized");
    }
}


nc_type Netcdf4FileWriter::to_nc(const DataType &type)
{
    if (type == (DataType::CHAR)) {
        return NC_CHAR;
    }
    else if (type == (DataType::SCHAR)) {
        return NC_BYTE;
    }
    else if (type == (DataType::SHORT)) {
#if   2 == SIZEOF_SHORT
        return NC_SHORT;
#elif 4 == SIZEOF_SHORT
        return NC_INT;
#else
        throw DataTypeException("could not determine nc_type from SHORT");
#endif
    }
    else if (type == (DataType::INT)) {
#if   2 == SIZEOF_INT
        return NC_SHORT;
#elif 4 == SIZEOF_INT
        return NC_INT;
#else
        throw DataTypeException("could not determine nc_type from INT");
#endif
    }
    else if (type == (DataType::LONG)) {
#if   2 == SIZEOF_LONG
        return NC_SHORT;
#elif 4 == SIZEOF_LONG
        return NC_INT;
#else
        throw DataTypeException("could not determine nc_type from LONG");
#endif
    }
    else if (type == (DataType::LONGLONG)) {
#if   2 == SIZEOF_LONG_LONG
        return NC_SHORT;
#elif 4 == SIZEOF_LONG_LONG
        return NC_INT;
#else
        throw DataTypeException("could not determine nc_type from LONGLONG");
#endif
    }
    else if (type == (DataType::FLOAT)) {
#if   4 == SIZEOF_FLOAT
        return NC_FLOAT;
#elif 8 == SIZEOF_FLOAT
        return NC_DOUBLE;
#else
        throw DataTypeException("could not determine nc_type from FLOAT");
#endif
    }
    else if (type == (DataType::DOUBLE)) {
#if   4 == SIZEOF_DOUBLE
        return NC_FLOAT;
#elif 8 == SIZEOF_DOUBLE
        return NC_DOUBLE;
#else
        throw DataTypeException("could not determine nc_type from DOUBLE");
#endif
    }
    else if (type == (DataType::LONGDOUBLE)) {
#if   4 == SIZEOF_LONG_DOUBLE
        return NC_FLOAT;
#elif 8 == SIZEOF_LONG_DOUBLE
        return NC_DOUBLE;
#else
        throw DataTypeException("could not determine nc_type from LONGDOUBLE");
#endif
    }
    else if (type == (DataType::UCHAR)) {
        throw DataTypeException("could not determine nc_type from UCHAR");
    }
    else if (type == (DataType::USHORT)) {
        throw DataTypeException("could not determine nc_type from USHORT");
    }
    else if (type == (DataType::UINT)) {
        throw DataTypeException("could not determine nc_type from UINT");
    }
    else if (type == (DataType::ULONG)) {
        throw DataTypeException("could not determine nc_type from ULONG");
    }
    else if (type == (DataType::ULONGLONG)) {
        throw DataTypeException("could not determine nc_type from ULONGLONG");
    }
    else if (type == (DataType::STRING)) {
        throw DataTypeException("could not determine nc_type from STRING");
    }

    throw DataTypeException("could not determine nc_type");
}


Netcdf4FileWriter::Netcdf4FileWriter(const string &filename)
    :   FileWriter()
    ,   is_in_define_mode(true)
    ,   filename(filename)
    ,   ncid(-1)
    ,   unlimdimid(-1)
    ,   _fixed_record_dimension(-1)
    ,   _header_pad(-1)
    ,   _file_format(FF_NETCDF4)
    ,   _append(false)
    ,   _overwrite(false)
    ,   dim_id()
    ,   dim_size()
    ,   var_id()
    ,   var_dims()
    ,   var_shape()
    ,   open(false)
{
}


Netcdf4FileWriter::~Netcdf4FileWriter()
{
    TIMING("Netcdf4FileWriter::~Netcdf4FileWriter()");
    close();
}


void Netcdf4FileWriter::close()
{
    if (open) {
        open = false;
        nc::close(ncid);
    }
}


FileWriter* Netcdf4FileWriter::create()
{
    MPI_Info info;

    TIMING("Netcdf4FileWriter::create()");
    TRACER("Netcdf4FileWriter create %s\n", filename.c_str());

    MPI_Info_create(&info);
    if (_header_pad > 0) {
        ostringstream value;
        value << _header_pad;
        MPI_Info_set(info, "nc_header_align_size",
                     const_cast<char*>(value.str().c_str()));
    }

    // create the output file
    if (pagoda::file_exists(filename)) {
        if (_append) {
            int ndims, nvars, ngatts;
            string name;
            size_t len;
            nc_type xtype;
            vector<int> dimids;
            int varnatts;

            ncid = nc::open(filename, NC_WRITE, MPI_COMM_WORLD, info);
            is_in_define_mode = false;
            // we are appending so we must learn all we can about this file
            // since later calls depend on knowledge of various IDs
            nc::inq(ncid, ndims, nvars, ngatts, unlimdimid);
            for (int dimid=0; dimid<ndims; ++dimid) {
                nc::inq_dim(ncid, dimid, name, len);
                dim_id[name] = dimid;
                dim_size[name] = len;
            }
            for (int varid=0; varid<nvars; ++varid) {
                vector<int64_t> shape;
                nc::inq_var(ncid, varid, name, xtype, dimids, varnatts);
                var_id[name] = varid;
                var_dims[name] = dimids;
                for (size_t dimidx=0; dimidx<dimids.size(); ++dimidx) {
                    string dim_name = nc::inq_dimname(ncid, dimids[dimidx]);
                    shape.push_back(get_dim_size(dim_name));
                }
                var_shape[name] = shape;
            }
        }
        else if (_overwrite) {
            ncid = nc::create(filename,
                    file_format_to_nc_format(_file_format)|NC_CLOBBER,
                    MPI_COMM_WORLD, info);
        }
        else {
            ERR("file exists");
        }
    }
    else {
        ncid = nc::create(filename, _file_format, MPI_COMM_WORLD, info);
    }

    open = true;
    MPI_Info_free(&info);
    return this;
}


FileWriter* Netcdf4FileWriter::fixed_record_dimension(int value)
{
    _fixed_record_dimension = value;
    return this;
}


FileWriter* Netcdf4FileWriter::header_pad(int value)
{
    _header_pad = value;
    return this;
}


FileWriter* Netcdf4FileWriter::file_format(FileFormat value)
{
    _file_format = value;
    return this;
}


FileWriter* Netcdf4FileWriter::append(bool value)
{
    _append = value;
    return this;
}


FileWriter* Netcdf4FileWriter::overwrite(bool value)
{
    _overwrite = value;
    return this;
}


void Netcdf4FileWriter::def_dim(const string &name, int64_t size)
{
    int id;

    TIMING("Netcdf4FileWriter::def_dim(Dimension*)");

    maybe_redef();

    if (size <= 0) {
        if (_fixed_record_dimension > 0) {
            size = _fixed_record_dimension;
        }
        else {
            size = NC_UNLIMITED;
        }
    }
    // if appending then ignore dim of same size if one already exists
    if (_append && (dim_id.find(name) != dim_id.end())) {
        // verify the new and old dim sizes match
        if (dim_id[name] != unlimdimid && size != dim_size[name]) {
            ostringstream strerr;
            strerr << "redef dim size mismatch: " << name;
            strerr << " old=" << dim_size[name] << " new=" << size;
            ERR(strerr.str());
        }
    }
    else {
        id = nc::def_dim(ncid, name, size);
        TRACER("Netcdf4FileWriter::def_dim %s=%lld id=%d\n",
               name.c_str(), size, id);
        dim_id[name] = id;
        dim_size[name] = size;
        if (size == NC_UNLIMITED) {
            unlimdimid = id;
        }
    }
}


void Netcdf4FileWriter::def_dim(Mask *mask)
{
    FileWriter::def_dim(mask);
}


void Netcdf4FileWriter::def_dim(Dimension *dim)
{
    FileWriter::def_dim(dim);
}


void Netcdf4FileWriter::def_var(const string &name,
                                const vector<string> &dims,
                                const DataType &type,
                                const vector<Attribute*> &atts)
{
    vector<int> dim_ids;
    vector<int64_t> shape;
    int id;

    TIMING("Netcdf4FileWriter::def_var(string,vector<string>)");

    maybe_redef();

    if (_append && (var_id.find(name) != var_id.end())) {
        // if appending and variable exists, overwrite attributes only
        write_atts_id(atts, var_id[name]);
    }
    else {
        for (int64_t i=0,limit=dims.size(); i<limit; ++i) {
            dim_ids.push_back(get_dim_id(dims.at(i)));
            shape.push_back(get_dim_size(dims.at(i)));
        }

        id = nc::def_var(ncid, name, to_nc(type), dim_ids);
        var_id[name] = id;
        var_dims[name] = dim_ids;
        var_shape[name] = shape;
        write_atts_id(atts, id);
    }
}


void Netcdf4FileWriter::def_var(const string &name,
                                const vector<Dimension*> &dims,
                                const DataType &type,
                                const vector<Attribute*> &atts)
{
    FileWriter::def_var(name, dims, type, atts);
}


void Netcdf4FileWriter::def_var(const string &name,
                                const vector<Mask*> &dims,
                                const DataType &type,
                                const vector<Attribute*> &atts)
{
    FileWriter::def_var(name, dims, type, atts);
}


void Netcdf4FileWriter::def_var(Variable *var)
{
    FileWriter::def_var(var);
}


ostream& Netcdf4FileWriter::print(ostream &os) const
{
    TIMING("Netcdf4FileWriter::print(ostream)");
    return os << "Netcdf4FileWriter(" << filename << ")";
}


int Netcdf4FileWriter::get_dim_id(const string &name) const
{
    ostringstream strerr;
    map<string,int>::const_iterator it = dim_id.find(name);

    TIMING("Netcdf4FileWriter::get_dim_id(string)");

    if (it != dim_id.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid dimension name";
    ERR(strerr.str());
}


int64_t Netcdf4FileWriter::get_dim_size(const string &name) const
{
    ostringstream strerr;
    map<string,int64_t>::const_iterator it = dim_size.find(name);

    TIMING("Netcdf4FileWriter::get_dim_size(string)");

    if (it != dim_size.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid dimension name";
    ERR(strerr.str());
}


int Netcdf4FileWriter::get_var_id(const string &name) const
{
    ostringstream strerr;
    map<string,int>::const_iterator it = var_id.find(name);

    TIMING("Netcdf4FileWriter::get_var_id(string)");

    if (it != var_id.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid variable name";
    ERR(strerr.str());
}


vector<int> Netcdf4FileWriter::get_var_dims(const string &name) const
{
    ostringstream strerr;
    map<string,vector<int> >::const_iterator it = var_dims.find(name);

    TIMING("Netcdf4FileWriter::get_var_dims(string)");

    if (it != var_dims.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid variable name";
    ERR(strerr.str());
}


vector<int64_t> Netcdf4FileWriter::get_var_shape(const string &name) const
{
    ostringstream strerr;
    map<string,vector<int64_t> >::const_iterator it = var_shape.find(name);

    TIMING("Netcdf4FileWriter::get_var_shape(string)");

    if (it != var_shape.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid variable name";
    ERR(strerr.str());
}


#if 0
void Netcdf4FileWriter::def_check() const
{
    TIMING("Netcdf4FileWriter::def_check()");
    if (!is_in_define_mode) {
        ostringstream strerr;
        strerr << "cannot (re)define output file after writing begins";
        ERR(strerr.str());
    }
}
#endif


void Netcdf4FileWriter::maybe_redef()
{
    TIMING("Netcdf4FileWriter::maybe_redef()");
    if (!is_in_define_mode) {
        is_in_define_mode = true;
        nc::redef(ncid);
        TRACER("Netcdf4FileWriter::maybe_redef BEGIN DEF\n");
    }
}


void Netcdf4FileWriter::maybe_enddef()
{
    TIMING("Netcdf4FileWriter::maybe_enddef()");
    if (is_in_define_mode) {
        is_in_define_mode = false;
        nc::enddef(ncid);
        TRACER("Netcdf4FileWriter::maybe_enddef END DEF\n");
    }
}


void Netcdf4FileWriter::write_att(const string &name, Values *values,
                                  DataType type,
                                  const string &var_name)
{
    FileWriter::write_att(name, values, type, var_name);
}


void Netcdf4FileWriter::write_att(Attribute *att, const string &name)
{
    TIMING("Netcdf4FileWriter::write_att(Attribute*,string)");
    write_att_id(att, name.empty() ? NC_GLOBAL : get_var_id(name));
}


void Netcdf4FileWriter::write_att_id(Attribute *attr, int varid)
{
    string name = attr->get_name();
    DataType dt = attr->get_type();

    TIMING("Netcdf4FileWriter::write_att_id(Attribute*,int)");
    TRACER("Netcdf4FileWriter::write_att_id %s\n", attr->get_name().c_str());

    maybe_redef();

#define put_attr_values(DT, CT) \
    if (dt == DT) { \
        vector<CT> data; \
        attr->get_values()->as(data); \
        nc::put_att(ncid, varid, name, data); \
    } else
    put_attr_values(DataType::UCHAR,  unsigned char)
    put_attr_values(DataType::SCHAR,  signed char)
    put_attr_values(DataType::CHAR,   char)
    put_attr_values(DataType::SHORT,  short)
    put_attr_values(DataType::INT,    int)
    put_attr_values(DataType::LONG,   long)
    put_attr_values(DataType::FLOAT,  float)
    put_attr_values(DataType::DOUBLE, double) {
        EXCEPT(DataTypeException, "DataType not handled", dt);
    }
#undef put_attr_values
}


void Netcdf4FileWriter::write_atts_id(const vector<Attribute*> &atts, int varid)
{
    //TIMING("Netcdf4FileWriter::write_atts_id(vector<Attribute*>,int)");
    vector<Attribute*>::const_iterator att_it;
    for (att_it=atts.begin(); att_it!=atts.end(); ++att_it) {
        write_att_id(*att_it, varid);
    }
}


void Netcdf4FileWriter::write(Array *array, const string &name)
{
    vector<int64_t> shape_to_compare = get_var_shape(name);
    vector<int64_t> start(shape_to_compare.size(), 0);

    // verify that the given array has the same shape as the defined variable
    // if we're dealing with an unlimited dimension, ignore that dimension
    if (shape_to_compare.size() > 0 && shape_to_compare[0] == 0) {
        vector<int64_t> compare_shape = shape_to_compare;
        vector<int64_t> array_shape = array->get_shape();
        compare_shape.erase(compare_shape.begin());
        array_shape.erase(array_shape.begin());
        if (compare_shape != array_shape) {
            ERR("shape mismatch");
        }
    }
    else {
        if (shape_to_compare != array->get_shape()) {
            ERR("shape mismatch");
        }
    }

    write(array, name, start);
}


void Netcdf4FileWriter::iwrite(Array *array, const string &name)
{
    write(array, name);
}


void Netcdf4FileWriter::write(Array *array, const string &name, int64_t record)
{
    DataType type = array->get_type();
    vector<int64_t> array_shape = array->get_shape();
    vector<int64_t> array_local_shape = array->get_local_shape();
    vector<int64_t> shape = get_var_shape(name);
    vector<size_t> start(shape.size(), 0);
    vector<size_t> count(shape.size(), 0);
    int var_id = get_var_id(name);

    TIMING("Netcdf4FileWriter::write(Array*,string,int64_t)");
    TRACER("Netcdf4FileWriter::write record %ld %s\n",
           (long)record, name.c_str());

    if (array_shape.size()+1 != shape.size()) {
        ERR("array rank mismatch");
    }
    shape.erase(shape.begin()); // remove record dimension
    if (array_shape != shape) {
        ERR("array shape mismatch");
    }

    maybe_enddef();

    if (array->owns_data()) {
        vector<int64_t> lo;
        vector<int64_t> hi;

        count[0] = 1;
        std::copy(array_local_shape.begin(), array_local_shape.end(),
                  count.begin()+1);
        start[0] = record;
        array->get_distribution(lo, hi);
        std::copy(lo.begin(), lo.end(), start.begin()+1);
    }

#define write_var(TYPE, DT) \
    if (type == DT) { \
        TYPE *ptr = NULL; \
        if (array->owns_data()) { \
            ptr = (TYPE*)array->access(); \
        } \
        nc::put_vara(ncid, var_id, start, count, ptr); \
        if (array->owns_data()) { \
            array->release(); \
        } \
    } else
    write_var(unsigned char, DataType::UCHAR)
    write_var(signed char,   DataType::SCHAR)
    write_var(char,          DataType::CHAR)
    write_var(int,           DataType::INT)
    write_var(long,          DataType::LONG)
    write_var(float,         DataType::FLOAT)
    write_var(double,        DataType::DOUBLE) {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
#undef write_var
}


void Netcdf4FileWriter::iwrite(Array *array, const string &name, int64_t record)
{
    write(array, name, record);
}


// it's a "patch" write.  the "hi" is implied by the shape of the given array
void Netcdf4FileWriter::write(Array *array, const string &name,
                              const vector<int64_t> &start)
{
    DataType type = array->get_type();
    vector<int64_t> array_shape = array->get_shape();
    vector<int64_t> array_local_shape = array->get_local_shape();
    vector<int64_t> array_lo;
    vector<int64_t> array_hi;
    vector<int64_t> shape = get_var_shape(name);
    vector<size_t> start_copy(start.begin(), start.end());
    vector<size_t> count(shape.size(), 0);
    int var_id = get_var_id(name);

    TIMING("Netcdf4FileWriter::write(Array*,string,vector<int64_t>)");
    TRACER("Netcdf4FileWriter::write %s\n", name.c_str());

    if (start.size() != shape.size()) {
        ERR("start rank mismatch");
    }
    if (array_shape.size() != shape.size()) {
        ERR("array rank mismatch");
    }

    maybe_enddef();

    if (array->owns_data()) {
        count.assign(array_local_shape.begin(), array_local_shape.end());
        array->get_distribution(array_lo, array_hi);
        for (size_t i=0; i<start_copy.size(); ++i) {
            start_copy[i] = start[i] + array_lo[i];
        }
    }

#define write_var(TYPE, DT) \
    if (type == DT) { \
        TYPE *ptr = NULL; \
        if (array->owns_data()) { \
            ptr = (TYPE*)array->access(); \
        } \
        nc::put_vara(ncid, var_id, start_copy, count, ptr); \
        if (array->owns_data()) { \
            array->release(); \
        } \
    } else
    write_var(unsigned char, DataType::UCHAR)
    write_var(signed char,   DataType::SCHAR)
    write_var(char,          DataType::CHAR)
    write_var(int,           DataType::INT)
    write_var(long,          DataType::LONG)
    write_var(float,         DataType::FLOAT)
    write_var(double,        DataType::DOUBLE) {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
#undef write_var
}


void Netcdf4FileWriter::iwrite(Array *array, const string &name,
                               const vector<int64_t> &start)
{
    write(array, name, start);
}


void Netcdf4FileWriter::wait()
{
}
