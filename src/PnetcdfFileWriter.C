#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cassert>
#include <exception>
#include <string>
#include <vector>

#include <pnetcdf.h>

#include "AbstractFileWriter.H"
#include "Attribute.H"
#include "Copy.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Mask.H"
#include "PnetcdfDimension.H"
#include "PnetcdfError.H"
#include "PnetcdfFileWriter.H"
#include "PnetcdfVariable.H"
#include "Pack.H"
#include "PnetcdfNS.H"
#include "Util.H"
#include "Values.H"

using std::invalid_argument;
using std::string;
using std::vector;


FileWriter* pagoda_pnetcdf_create(const string &filename)
{
    FileWriter *ret = NULL;

    try {
        ret = new PnetcdfFileWriter(filename);
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
    assert(FF_PNETCDF_CDF1 <= format && format <= FF_PNETCDF_CDF5);
    if (format == FF_PNETCDF_CDF1) {
        return 0; /* NC_FORMAT_CLASSIC is default */
    }
    else if (format == FF_PNETCDF_CDF2) {
        return NC_64BIT_OFFSET;
    }
    else if (format == FF_PNETCDF_CDF5) {
        return NC_64BIT_DATA;
    }
    else {
        ERR("FileFormat not recognized");
    }
}


nc_type PnetcdfFileWriter::to_nc(const DataType &type)
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


PnetcdfFileWriter::PnetcdfFileWriter(const string &filename)
    :   AbstractFileWriter()
    ,   is_in_define_mode(true)
    ,   filename(filename)
    ,   ncid(-1)
    ,   unlimdimid(-1)
    ,   _fixed_record_dimension(-1)
    ,   _header_pad(-1)
    ,   _file_format(FF_PNETCDF_CDF2)
    ,   _append(false)
    ,   _overwrite(false)
    ,   dim_id()
    ,   dim_size()
    ,   var_id()
    ,   var_dims()
    ,   var_shape()
    ,   nb_requests()
    ,   nb_arrays_to_release()
    ,   nb_buffers_to_delete()
    ,   open(false)
{
}


PnetcdfFileWriter::~PnetcdfFileWriter()
{
    close();
}


void PnetcdfFileWriter::close()
{
    if (open) {
        open = false;
        ncmpi::close(ncid);
    }
}


FileWriter* PnetcdfFileWriter::create()
{
    MPI_Info info;

    TRACER("PnetcdfFileWriter create %s\n", filename.c_str());

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
            MPI_Offset len;
            nc_type xtype;
            vector<int> dimids;
            int varnatts;

            ncid = ncmpi::open(MPI_COMM_WORLD, filename, NC_WRITE, info);
            is_in_define_mode = false;
            // we are appending so we must learn all we can about this file
            // since later calls depend on knowledge of various IDs
            ncmpi::inq(ncid, ndims, nvars, ngatts, unlimdimid);
            for (int dimid=0; dimid<ndims; ++dimid) {
                ncmpi::inq_dim(ncid, dimid, name, len);
                dim_id[name] = dimid;
                dim_size[name] = len;
            }
            for (int varid=0; varid<nvars; ++varid) {
                vector<int64_t> shape;
                ncmpi::inq_var(ncid, varid, name, xtype, dimids, varnatts);
                var_id[name] = varid;
                var_dims[name] = dimids;
                for (size_t dimidx=0; dimidx<dimids.size(); ++dimidx) {
                    string dim_name = ncmpi::inq_dimname(ncid, dimids[dimidx]);
                    shape.push_back(get_dim_size(dim_name));
                }
                var_shape[name] = shape;
            }
        }
        else if (_overwrite) {
            ncid = ncmpi::create(MPI_COMM_WORLD, filename,
                                 file_format_to_nc_format(_file_format)|NC_CLOBBER, info);
        }
        else {
            ERR("file exists");
        }
    }
    else {
        ncid = ncmpi::create(MPI_COMM_WORLD, filename, _file_format, info);
    }

    open = true;
    MPI_Info_free(&info);
    return this;
}


FileWriter* PnetcdfFileWriter::fixed_record_dimension(int value)
{
    _fixed_record_dimension = value;
    return this;
}


FileWriter* PnetcdfFileWriter::header_pad(int value)
{
    _header_pad = value;
    return this;
}


FileWriter* PnetcdfFileWriter::file_format(FileFormat value)
{
    _file_format = value;
    return this;
}


FileWriter* PnetcdfFileWriter::append(bool value)
{
    _append = value;
    return this;
}


FileWriter* PnetcdfFileWriter::overwrite(bool value)
{
    _overwrite = value;
    return this;
}


void PnetcdfFileWriter::def_dim(const string &name, int64_t size)
{
    int id;


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
        id = ncmpi::def_dim(ncid, name, size);
        TRACER("PnetcdfFileWriter::def_dim %s=%lld id=%d\n",
               name.c_str(), size, id);
        dim_id[name] = id;
        dim_size[name] = size;
        if (size == NC_UNLIMITED) {
            unlimdimid = id;
        }
    }
}


void PnetcdfFileWriter::def_dim(Mask *mask)
{
    AbstractFileWriter::def_dim(mask);
}


void PnetcdfFileWriter::def_dim(Dimension *dim)
{
    AbstractFileWriter::def_dim(dim);
}


void PnetcdfFileWriter::def_var(const string &name,
                                const vector<string> &dims,
                                const DataType &type,
                                const vector<Attribute*> &atts)
{
    vector<int> dim_ids;
    vector<int64_t> shape;
    int id;


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

        id = ncmpi::def_var(ncid, name, to_nc(type), dim_ids);
        var_id[name] = id;
        var_dims[name] = dim_ids;
        var_shape[name] = shape;
        write_atts_id(atts, id);
    }
}


void PnetcdfFileWriter::def_var(const string &name,
                                const vector<Dimension*> &dims,
                                const DataType &type,
                                const vector<Attribute*> &atts)
{
    AbstractFileWriter::def_var(name, dims, type, atts);
}


void PnetcdfFileWriter::def_var(const string &name,
                                const vector<Mask*> &dims,
                                const DataType &type,
                                const vector<Attribute*> &atts)
{
    AbstractFileWriter::def_var(name, dims, type, atts);
}


void PnetcdfFileWriter::def_var(Variable *var)
{
    AbstractFileWriter::def_var(var);
}


ostream& PnetcdfFileWriter::print(ostream &os) const
{
    return os << "PnetcdfFileWriter(" << filename << ")";
}


int PnetcdfFileWriter::get_dim_id(const string &name) const
{
    ostringstream strerr;
    map<string,int>::const_iterator it = dim_id.find(name);


    if (it != dim_id.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid dimension name";
    ERR(strerr.str());
}


int64_t PnetcdfFileWriter::get_dim_size(const string &name) const
{
    ostringstream strerr;
    map<string,int64_t>::const_iterator it = dim_size.find(name);


    if (it != dim_size.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid dimension name";
    ERR(strerr.str());
}


int PnetcdfFileWriter::get_var_id(const string &name) const
{
    ostringstream strerr;
    map<string,int>::const_iterator it = var_id.find(name);


    if (it != var_id.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid variable name";
    ERR(strerr.str());
}


vector<int> PnetcdfFileWriter::get_var_dims(const string &name) const
{
    ostringstream strerr;
    map<string,vector<int> >::const_iterator it = var_dims.find(name);


    if (it != var_dims.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid variable name";
    ERR(strerr.str());
}


vector<int64_t> PnetcdfFileWriter::get_var_shape(const string &name) const
{
    ostringstream strerr;
    map<string,vector<int64_t> >::const_iterator it = var_shape.find(name);


    if (it != var_shape.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid variable name";
    ERR(strerr.str());
}


#if 0
void PnetcdfFileWriter::def_check() const
{
    if (!is_in_define_mode) {
        ostringstream strerr;
        strerr << "cannot (re)define output file after writing begins";
        ERR(strerr.str());
    }
}
#endif


void PnetcdfFileWriter::maybe_redef()
{
    if (!is_in_define_mode) {
        is_in_define_mode = true;
        ncmpi::redef(ncid);
        TRACER("PnetcdfFileWriter::maybe_redef BEGIN DEF\n");
    }
}


void PnetcdfFileWriter::maybe_enddef()
{
    if (is_in_define_mode) {
        is_in_define_mode = false;
        ncmpi::enddef(ncid);
        TRACER("PnetcdfFileWriter::maybe_enddef END DEF\n");
    }
}


void PnetcdfFileWriter::write_att(const string &name, Values *values,
                                  DataType type,
                                  const string &var_name)
{
    AbstractFileWriter::write_att(name, values, type, var_name);
}


void PnetcdfFileWriter::write_att(Attribute *att, const string &name)
{
    write_att_id(att, name.empty() ? NC_GLOBAL : get_var_id(name));
}


void PnetcdfFileWriter::write_att_id(Attribute *attr, int varid)
{
    string name = attr->get_name();
    DataType dt = attr->get_type();

    TRACER("PnetcdfFileWriter::write_att_id %s\n", attr->get_name().c_str());

    maybe_redef();

#define put_attr_values(DT, CT) \
    if (dt == DT) { \
        vector<CT> data; \
        attr->get_values()->as(data); \
        ncmpi::put_att(ncid, varid, name, data); \
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


void PnetcdfFileWriter::write_atts_id(const vector<Attribute*> &atts, int varid)
{
    //TIMING("PnetcdfFileWriter::write_atts_id(vector<Attribute*>,int)");
    vector<Attribute*>::const_iterator att_it;
    for (att_it=atts.begin(); att_it!=atts.end(); ++att_it) {
        write_att_id(*att_it, varid);
    }
}


void PnetcdfFileWriter::write(Array *array, const string &name)
{
    write_wrapper(array, name, false);
}


void PnetcdfFileWriter::iwrite(Array *array, const string &name)
{
    write_wrapper(array, name, true);
}


void PnetcdfFileWriter::write_wrapper(Array *array, const string &name,
                                      bool nonblocking)
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

    write_wrapper(array, name, start, nonblocking);
}


void PnetcdfFileWriter::write(Array *array, const string &name, int64_t record)
{
    write_wrapper(array, name, record, false);
}


void PnetcdfFileWriter::iwrite(Array *array, const string &name, int64_t record)
{
    write_wrapper(array, name, record, true);
}


void PnetcdfFileWriter::write_wrapper(Array *array, const string &name,
        int64_t record, bool nonblocking)
{
    DataType type = array->get_type();
    vector<int64_t> array_shape = array->get_shape();
    vector<int64_t> array_local_shape = array->get_local_shape();
    vector<int64_t> shape = get_var_shape(name);
    vector<MPI_Offset> start(shape.size(), 0);
    vector<MPI_Offset> count(shape.size(), 0);
    int varid = get_var_id(name);

    TRACER("PnetcdfFileWriter::write record %ld %s\n",
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

    do_write(array, varid, start, count, nonblocking);
}


void PnetcdfFileWriter::write(Array *array, const string &name,
                              const vector<int64_t> &start)
{
    write_wrapper(array, name, start, false);
}


void PnetcdfFileWriter::iwrite(Array *array, const string &name,
                               const vector<int64_t> &start)
{
    write_wrapper(array, name, start, true);
}


// it's a "patch" write.  the "hi" is implied by the shape of the given array
void PnetcdfFileWriter::write_wrapper(Array *array, const string &name,
                                      const vector<int64_t> &start,
                                      bool nonblocking)
{
    DataType type = array->get_type();
    vector<int64_t> array_shape = array->get_shape();
    vector<int64_t> array_local_shape = array->get_local_shape();
    vector<int64_t> array_lo;
    vector<int64_t> array_hi;
    vector<int64_t> shape = get_var_shape(name);
    vector<MPI_Offset> start_copy(start.size(), 0);
    vector<MPI_Offset> count(shape.size(), 0);
    int varid = get_var_id(name);

    TRACER("PnetcdfFileWriter::write %s\n", name.c_str());

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

    do_write(array, varid, start_copy, count, nonblocking);
}


void PnetcdfFileWriter::do_write(Array *array, int varid,
                                 const vector<MPI_Offset> &start,
                                 const vector<MPI_Offset> &count,
                                 bool nonblocking)
{
    DataType type = array->get_type();
    DataType write_type = array->get_write_type();
    int64_t n = array->get_local_size();

#define write_var_all(TYPE, DT) \
    if (write_type == DT) { \
        TYPE *ptr = NULL; \
        if (array->owns_data()) { \
            if (write_type == type) { \
                ptr = static_cast<TYPE*>(array->access()); \
            } else { \
                void *gabuf = array->access(); \
                ptr = new TYPE[n]; \
                pagoda::copy(type, gabuf, write_type, ptr, n); \
                array->release(); \
            } \
        } \
        if (nonblocking) { \
            int request; \
            request = ncmpi::iput_vara(ncid, varid, start, count, ptr); \
            nb_requests.push_back(request); \
            if (write_type == type) { \
                nb_arrays_to_release.push_back(array); \
                nb_buffers_to_delete.push_back(NULL); \
            } else { \
                nb_arrays_to_release.push_back(array); \
                nb_buffers_to_delete.push_back(ptr); \
            } \
        } else { \
            ncmpi::put_vara_all(ncid, varid, start, count, ptr); \
            if (array->owns_data()) { \
                array->release(); \
            } \
        } \
    } else
    write_var_all(unsigned char, DataType::UCHAR)
    write_var_all(signed char,   DataType::SCHAR)
    write_var_all(char,          DataType::CHAR)
    write_var_all(int,           DataType::INT)
    write_var_all(long,          DataType::LONG)
    write_var_all(float,         DataType::FLOAT)
    write_var_all(double,        DataType::DOUBLE) {
        EXCEPT(DataTypeException, "DataType not handled", write_type);
    }
#undef write_var_all
}


void PnetcdfFileWriter::wait()
{

    if (!nb_requests.empty()) {
        vector<int> statuses(nb_requests.size());
        ncmpi::wait_all(ncid, nb_requests, statuses);
        // release Array pointers
        for (size_t i=0; i<nb_arrays_to_release.size(); ++i) {
            if (NULL != nb_buffers_to_delete[i]) {
                DataType type = nb_arrays_to_release[i]->get_write_type();
#define DATATYPE_EXPAND(DT,T) \
                if (DT == type) { \
                    T *buf = static_cast<T*>(nb_buffers_to_delete[i]); \
                    delete [] buf; \
                    buf = NULL; \
                } else
#include "DataType.def"
                {
                    EXCEPT(DataTypeException, "DataType not handled", type);
                }
                nb_buffers_to_delete[i] = NULL;
            } else {
                nb_arrays_to_release[i]->release();
            }
        }
        nb_requests.clear();
        nb_arrays_to_release.clear();
        nb_buffers_to_delete.clear();
    }
}

