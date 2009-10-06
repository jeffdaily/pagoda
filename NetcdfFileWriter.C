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
#include "DistributedMask.H"
#include "Mask.H"
#include "NetcdfDimension.H"
#include "NetcdfError.H"
#include "NetcdfFileWriter.H"
#include "NetcdfVariable.H"
#include "Pack.H"
#include "PnetcdfTiming.H"
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
{
    TIMING("NetcdfFileWriter::NetcdfFileWriter(string)");
    TRACER1("NetcdfFileWriter ctor(%s)\n", filename.c_str());
    // create the output file
    ncmpi::create(MPI_COMM_WORLD, filename.c_str(), NC_64BIT_OFFSET, MPI_INFO_NULL, &ncid);
    //ncmpi::create(MPI_COMM_WORLD, filename.c_str(), NC_64BIT_DATA, MPI_INFO_NULL, &ncid);
}


NetcdfFileWriter::~NetcdfFileWriter()
{
    TIMING("NetcdfFileWriter::~NetcdfFileWriter()");
    close(ncid);
}


void NetcdfFileWriter::def_dim(Dimension *dim)
{
    TIMING("NetcdfFileWriter::def_dim(Dimension*)");
    def_check();
    string name = dim->get_name();
    Mask *mask = dim->get_mask();
    MPI_Offset size;
    int id;

    if (dim->is_unlimited()) {
        size = NC_UNLIMITED;
    } else if (mask) {
        size = mask->get_count();
    } else {
        size = dim->get_size();
    }
    ncmpi::def_dim(ncid, name.c_str(), size, &id);
    TRACER3("NetcdfFileWriter::def_dim %s=%lld id=%d\n", name.c_str(), size, id);
    dim_id[name] = id;
}


void NetcdfFileWriter::def_var(Variable *var)
{
    TIMING("NetcdfFileWriter::def_var(Variable*)");
    def_check();
    string name = var->get_name();
    vector<Dimension*> dims = var->get_dims();
    int ndim = var->num_dims();
    int dim_ids[ndim];
    int id;
    nc_type type;

    for (int dimidx=0; dimidx<ndim; ++dimidx) {
        try {
            dim_ids[dimidx] = get_dim_id(dims.at(dimidx)->get_name());
        } catch (invalid_argument &ex) {
            TRACER("skipping variable without corresponding dim in output\n");
            return;
        }
    }
#ifdef TRACE
    if (1 == ndim) {
        TRACER2("NetcdfFileWriter::def_var %s(%d)\n", name.c_str(), dim_ids[0]);
    } else if (2 == ndim) {
        TRACER3("NetcdfFileWriter::def_var %s(%d,%d)\n", name.c_str(), dim_ids[0], dim_ids[1]);
    } else if (3 == ndim) {
        TRACER4("NetcdfFileWriter::def_var %s(%d,%d,%d)\n", name.c_str(), dim_ids[0], dim_ids[1], dim_ids[2]);
    }
#endif
    type = var->get_type();
    ncmpi::def_var(ncid, name.c_str(), type, ndim, dim_ids, &id);
    var_id[name] = id;

    copy_atts_id(var->get_atts(), id);
}


ostream& NetcdfFileWriter::print(ostream &os) const
{
    TIMING("NetcdfFileWriter::print(ostream)");
    return os << "NetcdfFileWriter(" << filename << ")";
}


int NetcdfFileWriter::get_dim_id(const string &name)
{
    TIMING("NetcdfFileWriter::get_dim_id(string)");
    map<string,int>::iterator it = dim_id.find(name);
    if (it != dim_id.end()) {
        return it->second;
    }
    ostringstream strerr;
    strerr << "'" << name << "' is not a valid dimension name";
    throw invalid_argument(strerr.str());
}


int NetcdfFileWriter::get_var_id(const string &name)
{
    TIMING("NetcdfFileWriter::get_var_id(string)");
    map<string,int>::iterator it = var_id.find(name);
    if (it != var_id.end()) {
        return it->second;
    }
    ostringstream strerr;
    strerr << "'" << name << "' is not a valid variable name";
    throw invalid_argument(strerr.str());
}


void NetcdfFileWriter::def_check()
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
    TIMING("NetcdfFileWriter::copy_att_id(Attribute*,int)");
    TRACER1("NetcdfFileWriter::copy_att_id %s\n", attr->get_name().c_str());
    def_check();
    string name = attr->get_name();
    DataType dt = attr->get_type();
    MPI_Offset len = attr->get_count();
#define put_attr_values(DT, CT) \
    if (dt == DT) { \
        nc_type type = DT; \
        vector<CT> data; \
        attr->get_values()->as(data); \
        CT *buf = &data[0]; \
        ncmpi::put_att(ncid, varid, name.c_str(), type, len, buf); \
    } else
    put_attr_values(NC_CHAR,   char)
    put_attr_values(NC_BYTE,   signed char)
    put_attr_values(NC_SHORT,  short)
    put_attr_values(NC_INT,    int)
    put_attr_values(NC_FLOAT,  float)
    put_attr_values(NC_DOUBLE, double)
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


void NetcdfFileWriter::write(int handle, const string &name, int record)
{
    //TIMING("NetcdfFileWriter::write(int,string,int)");
    maybe_enddef();
    write(handle, get_var_id(name), record);
}


void NetcdfFileWriter::write(int handle, int varid, int record)
{
    TIMING("NetcdfFileWriter::write(int,int,int)");
    TRACER3("NetcdfFileWriter::write %d %d %d\n", handle, varid, record);
    maybe_enddef();
    DataType type = NC_CHAR;
    int mt_type;
    int ndim;
    int64_t dim_sizes[GA_MAX_DIM];
    int64_t lo[GA_MAX_DIM];
    int64_t hi[GA_MAX_DIM];
    int64_t ld[GA_MAX_DIM-1];
    MPI_Offset start[GA_MAX_DIM];
    MPI_Offset count[GA_MAX_DIM];
    int dimidx=0;

    NGA_Inquire64(handle, &mt_type, &ndim, dim_sizes);
    type = mt_type;
    NGA_Distribution64(handle, ME, lo, hi);

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
