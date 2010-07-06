#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

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
#include "NetcdfDimension.H"
#include "NetcdfError.H"
#include "NetcdfFileWriter.H"
#include "NetcdfVariable.H"
#include "Pack.H"
#include "Pnetcdf.H"
#include "Util.H"
#include "Timing.H"
#include "Values.H"

using std::invalid_argument;
using std::string;
using std::vector;


NetcdfFileWriter::NetcdfFileWriter(const string &filename)
    :   FileWriter()
    ,   is_in_define_mode(true)
    ,   filename(filename)
    ,   ncid(-1)
    ,   dim_id()
    ,   dim_size()
    ,   var_id()
    ,   var_dims()
    ,   var_shape()
{
    TIMING("NetcdfFileWriter::NetcdfFileWriter(string)");
    TRACER("NetcdfFileWriter ctor(%s)\n", filename.c_str());
    // create the output file
    ncmpi::create(MPI_COMM_WORLD, filename.c_str(), NC_64BIT_OFFSET, MPI_INFO_NULL, &ncid);
    //ncmpi::create(MPI_COMM_WORLD, filename.c_str(), NC_64BIT_DATA, MPI_INFO_NULL, &ncid);
}


NetcdfFileWriter::~NetcdfFileWriter()
{
    TIMING("NetcdfFileWriter::~NetcdfFileWriter()");
    ncmpi::close(ncid);
}


void NetcdfFileWriter::def_dim(const string &name, int64_t size)
{
    int id;

    TIMING("NetcdfFileWriter::def_dim(Dimension*)");

    def_check();

    if (size <= 0) {
        size = NC_UNLIMITED;
    }

    ncmpi::def_dim(ncid, name.c_str(), size, &id);
    TRACER("NetcdfFileWriter::def_dim %s=%lld id=%d\n", name.c_str(), size, id);
    dim_id[name] = id;
    dim_size[name] = size;
}


void NetcdfFileWriter::def_var(const string &name,
        const vector<string> &dims,
        const DataType &type,
        const vector<Attribute*> &atts)
{
    int64_t ndim = dims.size();
    vector<int> dim_ids;
    vector<int64_t> shape;
    int id;

    TIMING("NetcdfFileWriter::def_var(string,vector<string>)");

    def_check();

    for (int64_t i=0; i<ndim; ++i) {
        dim_ids.push_back(get_dim_id(dims.at(i)));
        shape.push_back(get_dim_size(dims.at(i)));
    }

    ncmpi::def_var(ncid, name.c_str(), type.to_nc(), ndim, &dim_ids[0], &id);
    var_id[name] = id;
    var_dims[name] = dim_ids;
    var_shape[name] = shape;
    copy_atts_id(atts, id);
}


ostream& NetcdfFileWriter::print(ostream &os) const
{
    TIMING("NetcdfFileWriter::print(ostream)");
    return os << "NetcdfFileWriter(" << filename << ")";
}


int NetcdfFileWriter::get_dim_id(const string &name) const
{
    ostringstream strerr;
    map<string,int>::const_iterator it = dim_id.find(name);

    TIMING("NetcdfFileWriter::get_dim_id(string)");

    if (it != dim_id.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid dimension name";
    throw invalid_argument(strerr.str());
}


int64_t NetcdfFileWriter::get_dim_size(const string &name) const
{
    ostringstream strerr;
    map<string,int64_t>::const_iterator it = dim_size.find(name);

    TIMING("NetcdfFileWriter::get_dim_size(string)");

    if (it != dim_size.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid dimension name";
    throw invalid_argument(strerr.str());
}


int NetcdfFileWriter::get_var_id(const string &name) const
{
    ostringstream strerr;
    map<string,int>::const_iterator it = var_id.find(name);

    TIMING("NetcdfFileWriter::get_var_id(string)");

    if (it != var_id.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid variable name";
    throw invalid_argument(strerr.str());
}


vector<int> NetcdfFileWriter::get_var_dims(const string &name) const
{
    ostringstream strerr;
    map<string,vector<int> >::const_iterator it = var_dims.find(name);

    TIMING("NetcdfFileWriter::get_var_dims(string)");

    if (it != var_dims.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid variable name";
    throw invalid_argument(strerr.str());
}


vector<int64_t> NetcdfFileWriter::get_var_shape(const string &name) const
{
    ostringstream strerr;
    map<string,vector<int64_t> >::const_iterator it = var_shape.find(name);

    TIMING("NetcdfFileWriter::get_var_shape(string)");

    if (it != var_shape.end()) {
        return it->second;
    }

    strerr << "'" << name << "' is not a valid variable name";
    throw invalid_argument(strerr.str());
}


void NetcdfFileWriter::def_check() const
{
    TIMING("NetcdfFileWriter::def_check()");
    if (!is_in_define_mode) {
        ostringstream strerr;
        strerr << "cannot (re)define output file after writing begins";
        throw invalid_argument(strerr.str());
    }
}


void NetcdfFileWriter::maybe_enddef()
{
    TIMING("NetcdfFileWriter::maybe_enddef()");
    if (is_in_define_mode) {
        is_in_define_mode = false;
        ncmpi::enddef(ncid);
        TRACER("NetcdfFileWriter::maybe_enddef END DEF\n");
    }
}


void NetcdfFileWriter::copy_att(Attribute *att, const string &name)
{
    TIMING("NetcdfFileWriter::copy_att(Attribute*,string)");
    copy_att_id(att, name.empty() ? NC_GLOBAL : get_var_id(name));
}


void NetcdfFileWriter::copy_att_id(Attribute *attr, int varid)
{
    string name = attr->get_name();
    DataType dt = attr->get_type();
    MPI_Offset len = attr->get_count();

    TIMING("NetcdfFileWriter::copy_att_id(Attribute*,int)");
    TRACER("NetcdfFileWriter::copy_att_id %s\n", attr->get_name().c_str());

    def_check();

#define put_attr_values(DT, CT) \
    if (dt == DT) { \
        vector<CT> data; \
        attr->get_values()->as(data); \
        ncmpi::put_att(ncid, varid, name.c_str(), DT.to_nc(), len, &data[0]); \
    } else
    put_attr_values(DataType::CHAR,   char)
    put_attr_values(DataType::SHORT,  short)
    put_attr_values(DataType::INT,    int)
    put_attr_values(DataType::FLOAT,  float)
    put_attr_values(DataType::DOUBLE, double)
    ; // for last else above
#undef put_attr_values
}


void NetcdfFileWriter::copy_atts_id(const vector<Attribute*> &atts, int varid)
{
    //TIMING("NetcdfFileWriter::copy_atts_id(vector<Attribute*>,int)");
    vector<Attribute*>::const_iterator att_it;
    for (att_it=atts.begin(); att_it!=atts.end(); ++att_it) {
        copy_att_id(*att_it, varid);
    }
}

/*
void NetcdfFileWriter::write(int handle, const string &name, int record)
{
    //TIMING("NetcdfFileWriter::write(int,string,int)");
    maybe_enddef();
    write(handle, get_var_id(name), record);
}
*/


/*
void NetcdfFileWriter::write(int handle, int varid, int record)
{
    DataType type = NC_CHAR;
    int mt_type;
    int ndim;
    int64_t dim_sizes_[GA_MAX_DIM];
    int64_t lo[GA_MAX_DIM];
    int64_t hi[GA_MAX_DIM];
    int64_t ld[GA_MAX_DIM-1];
    MPI_Offset start[GA_MAX_DIM];
    MPI_Offset count[GA_MAX_DIM];
    int dimidx=0;

    TIMING("NetcdfFileWriter::write(int,int,int)");
    TRACER("NetcdfFileWriter::write %d %d %d\n", handle, varid, record);

    maybe_enddef();

    NGA_Inquire64(handle, &mt_type, &ndim, dim_sizes_);
    type = mt_type;
    NGA_Distribution64(handle, GA_Nodeid(), lo, hi);

    if (0 > lo[0] && 0 > hi[0]) {
        // make a non-participating process a no-op
        for (dimidx=0; dimidx<ndim; ++dimidx) {
            start[dimidx] = 0;
            count[dimidx] = 0;
        }
#define write_var_all(TYPE, NC_TYPE) \
        if (type == NC_TYPE) { \
            ncmpi::put_vara_all(ncid, varid, start, count, (const TYPE*)NULL);\
        } else
        write_var_all(int,    NC_INT)
        write_var_all(float,  NC_FLOAT)
        write_var_all(double, NC_DOUBLE)
#undef write_var_all
        ; // for last else above
    } else {
        if (record >= 0) {
            start[0] = record;
            count[0] = 1;
            for (dimidx=0; dimidx<ndim; ++dimidx) {
                start[dimidx+1] = lo[dimidx];
                count[dimidx+1] = hi[dimidx] - lo[dimidx] + 1;
            }
        } else {
            for (dimidx=0; dimidx<ndim; ++dimidx) {
                start[dimidx] = lo[dimidx];
                count[dimidx] = hi[dimidx] - lo[dimidx] + 1;
            }
        }
#define write_var_all(TYPE, NC_TYPE) \
        if (type == NC_TYPE) { \
            TYPE *ptr; \
            NGA_Access64(handle, lo, hi, &ptr, ld); \
            ncmpi::put_vara_all(ncid, varid, start, count, ptr); \
        } else
        write_var_all(int, NC_INT)
        write_var_all(float, NC_FLOAT)
        write_var_all(double, NC_DOUBLE)
#undef write_var_all
        ; // for last else above
        NGA_Release64(handle, lo, hi);
    }
}
*/


void NetcdfFileWriter::write(Array *array, const string &name)
{
    vector<int64_t> shape_to_compare = get_var_shape(name);
    vector<int64_t> start(shape_to_compare.size(), 0);

    // verify that the given array has the same shape as the defined variable
    if (shape_to_compare != array->get_shape()) {
        throw invalid_argument("shape mismatch");
    }

    write(array, name, start);
}


void NetcdfFileWriter::write(Array *array, const string &name, int64_t record)
{
    vector<int64_t> shape_to_compare = get_var_shape(name);
    vector<int64_t> start(shape_to_compare.size(), 0);

    // remove record part of shape
    shape_to_compare.erase(shape_to_compare.begin());
    // verify that the given array has the same shape as the defined variable
    if (shape_to_compare != array->get_shape()) {
        throw invalid_argument("shape mismatch");
    }

    start[0] = record;

    write(array, name, start);
}


// it's a "patch" write.  the "hi" is implied by the shape of the given array
void NetcdfFileWriter::write(Array *array, const string &name,
        const vector<int64_t> &start)
{
    DataType type = array->get_type();
    vector<int64_t> array_shape = array->get_shape();
    vector<int64_t> array_local_shape = array->get_local_shape();
    vector<int64_t> var_shape = get_var_shape(name);
    vector<MPI_Offset> start_copy(start.begin(), start.end());
    vector<MPI_Offset> count(var_shape.size(), 0);
    int var_id = get_var_id(name);

    TIMING("NetcdfFileWriter::write(Array*,string,vector<int64_t>)");
    TRACER("NetcdfFileWriter::write %s\n", name.c_str());

    if (start_copy.size() != var_shape.size()) {
        throw invalid_argument("start rank mismatch");
    }
    if (array_shape.size() != var_shape.size()) {
        throw invalid_argument("array rank mismatch");
    }

    maybe_enddef();

    if (array->owns_data()) {
        count.assign(array_local_shape.begin(), array_local_shape.end());
    } else {
        // make a non-participating process a no-op
        count.assign(var_shape.size(), 0);
    }

#define write_var_all(TYPE, DT) \
    if (type == DT) { \
        TYPE *ptr = NULL; \
        if (array->owns_data()) { \
            ptr = (TYPE*)array->access(); \
        } \
        ncmpi::put_vara_all(ncid, var_id, &start_copy[0], &count[0], ptr); \
        if (array->owns_data()) { \
            array->release(); \
        } \
    } else
    write_var_all(int,    DataType::INT)
    write_var_all(float,  DataType::FLOAT)
    write_var_all(double, DataType::DOUBLE)
    {
        throw invalid_argument("unrecognized DataType");
    }
#undef write_var_all
}
