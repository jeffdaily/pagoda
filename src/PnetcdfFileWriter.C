#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cassert>
#include <exception>
#include <string>
#include <vector>

#include <pnetcdf.h>

#include "Attribute.H"
#include "ConnectivityVariable.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "Mask.H"
#include "PnetcdfDimension.H"
#include "PnetcdfError.H"
#include "PnetcdfFileWriter.H"
#include "PnetcdfVariable.H"
#include "Pack.H"
#include "Pnetcdf.H"
#include "Util.H"
#include "Timing.H"
#include "Values.H"

using std::invalid_argument;
using std::string;
using std::vector;


static int file_format_to_nc_format(FileFormat format)
{
    assert(FF_PNETCDF_CDF1 <= format && format <= FF_PNETCDF_CDF5);
    if (format == FF_PNETCDF_CDF1) {
        return 0; /* NC_FORMAT_CLASSIC is default */
    } else if (format == FF_PNETCDF_CDF2) {
        return NC_64BIT_OFFSET;
    } else if (format == FF_PNETCDF_CDF5) {
        return NC_64BIT_DATA;
    }
}


PnetcdfFileWriter::PnetcdfFileWriter(const string &filename, bool _append)
    :   FileWriter()
    ,   is_in_define_mode(true)
    ,   filename(filename)
    ,   ncid(-1)
    ,   unlimdimid(-1)
    ,   append(_append)
    ,   fixed_record_dimension_size(0)
    ,   dim_id()
    ,   dim_size()
    ,   var_id()
    ,   var_dims()
    ,   var_shape()
    ,   open(true)
{
    TIMING("PnetcdfFileWriter::PnetcdfFileWriter(string)");
    TRACER("PnetcdfFileWriter ctor(%s)\n", filename.c_str());
    // create the output file
    if (append) {
        int ndims, nvars, ngatts;
        string name;
        MPI_Offset len;
        nc_type xtype;
        vector<int> dimids;
        int varnatts;

        ncid = ncmpi::open(MPI_COMM_WORLD, filename, NC_WRITE, MPI_INFO_NULL);
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
            for (int dimidx=0; dimidx<dimids.size(); ++dimidx) {
                string dim_name = ncmpi::inq_dimname(ncid, dimids[dimidx]);
                shape.push_back(get_dim_size(dim_name));
            }
            var_shape[name] = shape;
        }
    } else {
        ncid = ncmpi::create(MPI_COMM_WORLD, filename, NC_64BIT_OFFSET,
                MPI_INFO_NULL);
    }
}


PnetcdfFileWriter::PnetcdfFileWriter(const string &filename, FileFormat format)
    :   FileWriter()
    ,   is_in_define_mode(true)
    ,   filename(filename)
    ,   ncid(-1)
    ,   unlimdimid(-1)
    ,   append(false)
    ,   fixed_record_dimension_size(0)
    ,   dim_id()
    ,   dim_size()
    ,   var_id()
    ,   var_dims()
    ,   var_shape()
    ,   open(true)
{
    TIMING("PnetcdfFileWriter::PnetcdfFileWriter(string,FileFormat)");
    TRACER("PnetcdfFileWriter ctor(%s)\n", filename.c_str());
    // create the output file
    int ncformat = file_format_to_nc_format(format);
    ncid = ncmpi::create(MPI_COMM_WORLD, filename, ncformat, MPI_INFO_NULL);
}


PnetcdfFileWriter::~PnetcdfFileWriter()
{
    TIMING("PnetcdfFileWriter::~PnetcdfFileWriter()");
    close();
}


void PnetcdfFileWriter::close()
{
    if (open) {
        open = false;
        ncmpi::close(ncid);
    }
}


void PnetcdfFileWriter::set_fixed_record_dimension(int size)
{
    fixed_record_dimension_size = size;
}


void PnetcdfFileWriter::def_dim(const string &name, int64_t size)
{
    int id;

    TIMING("PnetcdfFileWriter::def_dim(Dimension*)");

    maybe_redef();

    if (size <= 0) {
        if (fixed_record_dimension_size) {
            size = fixed_record_dimension_size;
        } else {
            size = NC_UNLIMITED;
        }
    }
    // if appending then ignore dim of same size if one already exists
    if (append && (dim_id.find(name) != dim_id.end())) {
        // verify the new and old dim sizes match
        if (dim_id[name] != unlimdimid && size != dim_size[name]) {
            ostringstream strerr;
            strerr << "redef dim size mismatch: " << name;
            strerr << " old=" << dim_size[name] << " new=" << size;
            throw PagodaException(strerr.str());
        }
    } else {
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


void PnetcdfFileWriter::def_var(const string &name,
        const vector<string> &dims,
        const DataType &type,
        const vector<Attribute*> &atts)
{
    vector<int> dim_ids;
    vector<int64_t> shape;
    int id;

    TIMING("PnetcdfFileWriter::def_var(string,vector<string>)");

    maybe_redef();

    if (append && (var_id.find(name) != var_id.end())) {
        // if appending and variable exists, overwrite attributes only
        write_atts_id(atts, var_id[name]);
    } else {
        for (int64_t i=0,limit=dims.size(); i<limit; ++i) {
            dim_ids.push_back(get_dim_id(dims.at(i)));
            shape.push_back(get_dim_size(dims.at(i)));
        }

        id = ncmpi::def_var(ncid, name, type.to_nc(), dim_ids);
        var_id[name] = id;
        var_dims[name] = dim_ids;
        var_shape[name] = shape;
        write_atts_id(atts, id);
    }
}


ostream& PnetcdfFileWriter::print(ostream &os) const
{
    TIMING("PnetcdfFileWriter::print(ostream)");
    return os << "PnetcdfFileWriter(" << filename << ")";
}


int PnetcdfFileWriter::get_dim_id(const string &name) const
{
    ostringstream strerr;
    map<string,int>::const_iterator it = dim_id.find(name);

    TIMING("PnetcdfFileWriter::get_dim_id(string)");

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

    TIMING("PnetcdfFileWriter::get_dim_size(string)");

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

    TIMING("PnetcdfFileWriter::get_var_id(string)");

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

    TIMING("PnetcdfFileWriter::get_var_dims(string)");

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

    TIMING("PnetcdfFileWriter::get_var_shape(string)");

    if (it != var_shape.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid variable name";
    ERR(strerr.str());
}


#if 0
void PnetcdfFileWriter::def_check() const
{
    TIMING("PnetcdfFileWriter::def_check()");
    if (!is_in_define_mode) {
        ostringstream strerr;
        strerr << "cannot (re)define output file after writing begins";
        ERR(strerr.str());
    }
}
#endif


void PnetcdfFileWriter::maybe_redef()
{
    TIMING("PnetcdfFileWriter::maybe_redef()");
    if (!is_in_define_mode) {
        is_in_define_mode = true;
        ncmpi::redef(ncid);
        TRACER("PnetcdfFileWriter::maybe_redef BEGIN DEF\n");
    }
}


void PnetcdfFileWriter::maybe_enddef()
{
    TIMING("PnetcdfFileWriter::maybe_enddef()");
    if (is_in_define_mode) {
        is_in_define_mode = false;
        ncmpi::enddef(ncid);
        TRACER("PnetcdfFileWriter::maybe_enddef END DEF\n");
    }
}


void PnetcdfFileWriter::write_att(Attribute *att, const string &name)
{
    TIMING("PnetcdfFileWriter::write_att(Attribute*,string)");
    write_att_id(att, name.empty() ? NC_GLOBAL : get_var_id(name));
}


void PnetcdfFileWriter::write_att_id(Attribute *attr, int varid)
{
    string name = attr->get_name();
    DataType dt = attr->get_type();
    MPI_Offset len = attr->get_count();

    TIMING("PnetcdfFileWriter::write_att_id(Attribute*,int)");
    TRACER("PnetcdfFileWriter::write_att_id %s\n", attr->get_name().c_str());

    maybe_redef();

#define put_attr_values(DT, CT) \
    if (dt == DT) { \
        vector<CT> data; \
        attr->get_values()->as(data); \
        ncmpi::put_att(ncid, varid, name, data); \
    } else
    put_attr_values(DataType::CHAR,   char)
    put_attr_values(DataType::SHORT,  short)
    put_attr_values(DataType::INT,    int)
    put_attr_values(DataType::FLOAT,  float)
    put_attr_values(DataType::DOUBLE, double)
    {
        throw DataTypeException("DataType not handled", dt);
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
    } else {
        if (shape_to_compare != array->get_shape()) {
            ERR("shape mismatch");
        }
    }

    write(array, name, start);
}


void PnetcdfFileWriter::write(Array *array, const string &name, int64_t record)
{
    DataType type = array->get_type();
    vector<int64_t> array_shape = array->get_shape();
    vector<int64_t> array_local_shape = array->get_local_shape();
    vector<int64_t> shape = get_var_shape(name);
    vector<MPI_Offset> start(shape.size(), 0);
    vector<MPI_Offset> count(shape.size(), 0);
    int var_id = get_var_id(name);

    TIMING("PnetcdfFileWriter::write(Array*,string,int64_t)");
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

#define write_var_all(TYPE, DT) \
    if (type == DT) { \
        TYPE *ptr = NULL; \
        if (array->owns_data()) { \
            ptr = (TYPE*)array->access(); \
        } \
        ncmpi::put_vara_all(ncid, var_id, start, count, ptr); \
        if (array->owns_data()) { \
            array->release(); \
        } \
    } else
    write_var_all(int,    DataType::INT)
    write_var_all(float,  DataType::FLOAT)
    write_var_all(double, DataType::DOUBLE)
    {
        throw DataTypeException("DataType not handled", type);
    }
#undef write_var_all
}


// it's a "patch" write.  the "hi" is implied by the shape of the given array
void PnetcdfFileWriter::write(Array *array, const string &name,
        const vector<int64_t> &start)
{
    DataType type = array->get_type();
    vector<int64_t> array_shape = array->get_shape();
    vector<int64_t> array_local_shape = array->get_local_shape();
    vector<int64_t> array_lo;
    vector<int64_t> array_hi;
    vector<int64_t> shape = get_var_shape(name);
    vector<MPI_Offset> start_copy(start.begin(), start.end());
    vector<MPI_Offset> count(shape.size(), 0);
    int var_id = get_var_id(name);

    TIMING("PnetcdfFileWriter::write(Array*,string,vector<int64_t>)");
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

#define write_var_all(TYPE, DT) \
    if (type == DT) { \
        TYPE *ptr = NULL; \
        if (array->owns_data()) { \
            ptr = (TYPE*)array->access(); \
        } \
        ncmpi::put_vara_all(ncid, var_id, start_copy, count, ptr); \
        if (array->owns_data()) { \
            array->release(); \
        } \
    } else
    write_var_all(int,    DataType::INT)
    write_var_all(float,  DataType::FLOAT)
    write_var_all(double, DataType::DOUBLE)
    {
        throw DataTypeException("DataType not handled", type);
    }
#undef write_var_all
}
