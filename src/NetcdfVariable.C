#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <ga.h>
#include <pnetcdf.h>

#include "Attribute.H"
#include "Common.H"
#include "Debug.H"
#include "Mask.H"
#include "NetcdfAttribute.H"
#include "NetcdfDataset.H"
#include "NetcdfDimension.H"
#include "NetcdfError.H"
#include "NetcdfVariable.H"
#include "Pnetcdf.H"
#include "Timing.H"
#include "Util.H"


NetcdfVariable::NetcdfVariable(NetcdfDataset *dataset, int varid)
    :   AbstractVariable()
    ,   dataset(dataset)
    ,   id(varid)
    ,   name("")
    ,   dims()
    ,   atts()
    ,   type(NC_CHAR)
{
    TIMING("NetcdfVariable::NetcdfVariable(NetcdfDataset*,int)");
    int ncid = dataset->get_id();
    int ndim;
    ncmpi::inq_ndims(ncid, &ndim);
    char cname[MAX_NAME];
    int dim_ids[ndim]; // plenty big (file's ndim <= variable's ndim)
    nc_type type_tmp;
    int natt;
    ncmpi::inq_var(ncid, varid, cname, &type_tmp, &ndim, dim_ids, &natt);
    name = string(cname);
    type = type_tmp;
    for (int dimidx=0; dimidx<ndim; ++dimidx) {
        dims.push_back(dataset->get_dim(dim_ids[dimidx]));
    }
    for (int attid=0; attid<natt; ++attid) {
        atts.push_back(new NetcdfAttribute(dataset, attid, this));
    }
}


NetcdfVariable::~NetcdfVariable()
{
    TIMING("NetcdfVariable::~NetcdfVariable()");
}


string NetcdfVariable::get_name() const
{
    TIMING("NetcdfVariable::get_name()");
    return name;
}


vector<Dimension*> NetcdfVariable::get_dims() const
{
    TIMING("NetcdfVariable::get_dims()");
    return vector<Dimension*>(dims.begin(), dims.end());
}


vector<Attribute*> NetcdfVariable::get_atts() const
{
    TIMING("NetcdfVariable::get_atts()");
    return vector<Attribute*>(atts.begin(), atts.end());
}


DataType NetcdfVariable::get_type() const
{
    TIMING("NetcdfVariable::get_type()");
    return type;
}


/*
void NetcdfVariable::read()
{
    TRACER("NetcdfVariable::read() %s\n", get_name().c_str());
    TIMING("NetcdfVariable::read()");
    int me = GA_Nodeid();
    int ncid = dataset->get_id();
    DataType type = get_type();
    size_t ndim = num_dims();
    size_t ndim_fixed = ndim;
    size_t dimidx = 0;
    int64_t lo[ndim];
    int64_t hi[ndim];
    int64_t ld[ndim-1];
    MPI_Offset start[ndim];
    MPI_Offset count[ndim];

    if (has_record() && ndim > 1) {
        --ndim_fixed;
    }

    NGA_Distribution64(get_handle(), me, lo, hi);
#ifdef TRACE
    if (ndim_fixed == 1) {
        PRINT_SYNC("lo={%ld}\thi={%ld}\n",
                lo[0], hi[0]);
    } else if (ndim_fixed == 2) {
        PRINT_SYNC("lo={%ld,%ld}\thi={%ld,%ld}\n",
                lo[0], lo[1], hi[0], hi[1]);
    } else if (ndim_fixed == 3) {
        PRINT_SYNC("lo={%ld,%ld,%ld}\thi={%ld,%ld,%ld}\n",
                lo[0], lo[1], lo[2], hi[0], hi[1], hi[2]);
    } else if (ndim_fixed == 4) {
        PRINT_SYNC("lo={%ld,%ld,%ld,%ld}\thi={%ld,%ld,%ld,%ld}\n",
                lo[0], lo[1], lo[2], lo[3], hi[0], hi[1], hi[2], hi[3]);
    }
#endif

#ifdef READ_OPT
    bool found_bit = true;
    // only attempt the optimization for record variables
    if (has_record() && ndim > 1) {
        PRINT_ZERO("\nattempting READ_OPT for %s\n", get_name().c_str());
        if (0 > lo[0] && 0 > hi[0]) {
            PRINT_SYNC("found_bit=non-participating\n");
        } else {
            for (size_t i=0; i<ndim_fixed; ++i) {
                bool current_found_bit = false;
                Dimension *dim = dims[i+1];
                vector<int> data;
                dim->get_mask()->get_data(data, lo[i], hi[i]);
                for (size_t j=0,limit=data.size(); j<limit; ++j) {
                    if (data[j] != 0) {
                        current_found_bit = true;
                        break;
                    }
                }
                found_bit &= current_found_bit;
            }
            if (found_bit) {
                PRINT_SYNC("found_bit=true\n");
            } else {
                PRINT_SYNC("found_bit=false\n");
            }
        }
    }
    if ((0 > lo[0] && 0 > hi[0]) || !found_bit)
#else
    if (0 > lo[0] && 0 > hi[0])
#endif
    {
        // make a non-participating process a no-op
        for (dimidx=0; dimidx<ndim; ++dimidx) {
            start[dimidx] = 0;
            count[dimidx] = 0;
        }
#ifdef TRACE
        if (ndim == 2) {
            PRINT_SYNC("start={%ld,%ld}\tcount={%ld,%ld}\n",
                    start[0], start[1], count[0], count[1]);
        } else if (ndim == 3) {
            PRINT_SYNC("start={%ld,%ld,%ld}\tcount={%ld,%ld,%ld}\n",
                    start[0], start[1], start[2],
                    count[0], count[1], count[2]);
        } else if (ndim == 4) {
            PRINT_SYNC("start={%ld,%ld,%ld,%ld}\tcount={%ld,%ld,%ld,%ld}\n",
                    start[0], start[1], start[2], start[3],
                    count[0], count[1], count[2], count[3]);
        }
#endif
#define read_var_all(TYPE, NC_TYPE) \
        if (type == NC_TYPE) { \
            ncmpi::get_vara_all(ncid, id, start, count, (TYPE*)NULL); \
            TRACER("no-op\n"); \
        } else
        read_var_all(int,    NC_INT)
        read_var_all(float,  NC_FLOAT)
        read_var_all(double, NC_DOUBLE)
        ; // for last else above
#undef read_var_all
    } else {
        if (has_record() && ndim > 1) {
            start[0] = record_index;
            count[0] = 1;
            for (dimidx=1; dimidx<ndim; ++dimidx) {
                start[dimidx] = lo[dimidx-1];
                count[dimidx] = hi[dimidx-1] - lo[dimidx-1] + 1;
            }
        } else {
            for (dimidx=0; dimidx<ndim; ++dimidx) {
                start[dimidx] = lo[dimidx];
                count[dimidx] = hi[dimidx] - lo[dimidx] + 1;
            }
        }
#ifdef TRACE
        if (ndim == 2) {
            PRINT_SYNC("start={%ld,%ld}\tcount={%ld,%ld}\n",
                    start[0], start[1], count[0], count[1]);
        } else if (ndim == 3) {
            PRINT_SYNC("start={%ld,%ld,%ld}\tcount={%ld,%ld,%ld}\n",
                    start[0], start[1], start[2],
                    count[0], count[1], count[2]);
        } else if (ndim == 4) {
            PRINT_SYNC("start={%ld,%ld,%ld,%ld}\tcount={%ld,%ld,%ld,%ld}\n",
                    start[0], start[1], start[2], start[3],
                    count[0], count[1], count[2], count[3]);
        }
#endif
#define read_var_all(TYPE, NC_TYPE, FMT) \
        if (type == NC_TYPE) { \
            TYPE *ptr; \
            NGA_Access64(get_handle(), lo, hi, &ptr, ld); \
            ncmpi::get_vara_all(ncid, id, start, count, ptr); \
            TRACER("ptr[0]="#FMT"\n", ptr[0]); \
            if (ndim_fixed == 2) { \
                TRACER("ld={%ld}\n", ld[0]); \
            } else if (ndim_fixed == 3) { \
                TRACER("ld={%ld,%ld}\n", ld[0], ld[1]); \
            } else if (ndim_fixed == 4) { \
                TRACER("ld={%ld,%ld,%ld}\n", ld[0], ld[1], ld[2]); \
            } \
        } else
        read_var_all(int, NC_INT, %d)
        read_var_all(float, NC_FLOAT, %f)
        read_var_all(double, NC_DOUBLE, %f)
        ; // for last else above
#undef read_var_all
        NGA_Release_update64(get_handle(), lo, hi);
    }
}
*/


Array* NetcdfVariable::read(Array *dst)
{
    int ncid = dataset->get_id();
    DataType type = get_type();
    int64_t ndim = num_dims();
    vector<int64_t> lo(ndim);
    vector<int64_t> hi(ndim);
    vector<MPI_Offset> start(ndim);
    vector<MPI_Offset> count(ndim);
    bool found_bit = true;

    TRACER("NetcdfVariable::read(Array*) %s\n", get_name().c_str());
    TIMING("NetcdfVariable::read(Array*)");

    dst->get_distribution(lo,hi);

    if (dst->get_ndim() != ndim) {
        Util::abort("NetcdfVariable::read(Array*) :: shape mismatch");
    }

#ifdef READ_OPT
#   error TODO read optimization
    // only attempt the optimization for record variables
    // **Why did I decide to do it that way??**
    // check all dimension masks for a non-zero bit
#endif

    if (dst->owns_data() && found_bit) {
        for (int64_t dimidx=0; dimidx<ndim; ++dimidx) {
            start[dimidx] = lo[dimidx];
            count[dimidx] = hi[dimidx] - lo[dimidx] + 1;
        }
#define read_var_all(TYPE, DT) \
        if (type == DT) { \
            TYPE *ptr = NULL; \
            ptr = (TYPE*)dst->access(); \
            ncmpi::get_vara_all(ncid, id, &start[0], &count[0], ptr); \
        } else
        read_var_all(int,    DataType::INT)
        read_var_all(float,  DataType::FLOAT)
        read_var_all(double, DataType::DOUBLE)
        ; // for last else above
        dst->release();
#undef read_var_all
    } else {
        // make a non-participating process a no-op
        fill(start.begin(), start.end(), 0);
        fill(count.begin(), count.end(), 0);
#define read_var_all(TYPE, DT) \
        if (type == DT) { \
            TYPE *ptr = NULL; \
            ncmpi::get_vara_all(ncid, id, &start[0], &count[0], ptr); \
        } else
        read_var_all(int,    DataType::INT)
        read_var_all(float,  DataType::FLOAT)
        read_var_all(double, DataType::DOUBLE)
        ; // for last else above
#undef read_var_all
    }

    return dst;
}


Array* NetcdfVariable::read(int64_t record, Array *dst)
{
    int ncid = dataset->get_id();
    DataType type = get_type();
    int64_t ndim = num_dims();
    vector<int64_t> lo(ndim);
    vector<int64_t> hi(ndim);
    vector<MPI_Offset> start(ndim);
    vector<MPI_Offset> count(ndim);
    bool found_bit = true;

    TRACER("NetcdfVariable::read(int64_t,Array*) %s\n", get_name().c_str());
    TIMING("NetcdfVariable::read(int64_t,Array*)");

    dst->get_distribution(lo,hi);

    if (dst->get_ndim()+1 != ndim) {
        Util::abort("NetcdfVariable::read(int64_t,Array*) :: shape mismatch");
    }

#ifdef READ_OPT
#   error TODO read optimization
    // only attempt the optimization for record variables
    // **Why did I decide to do it that way??**
    // check all dimension masks for a non-zero bit
#endif

    if (dst->owns_data() && found_bit) {
        start[0] = record;
        count[0] = 1;
        for (int64_t dimidx=1; dimidx<ndim; ++dimidx) {
            start[dimidx] = lo[dimidx-1];
            count[dimidx] = hi[dimidx-1] - lo[dimidx-1] + 1;
        }
#define read_var_all(TYPE, DT) \
        if (type == DT) { \
            TYPE *ptr = NULL; \
            ptr = (TYPE*)dst->access(); \
            ncmpi::get_vara_all(ncid, id, &start[0], &count[0], ptr); \
        } else
        read_var_all(int,    DataType::INT)
        read_var_all(float,  DataType::FLOAT)
        read_var_all(double, DataType::DOUBLE)
        ; // for last else above
        dst->release();
#undef read_var_all
    } else {
        // make a non-participating process a no-op
        fill(start.begin(), start.end(), 0);
        fill(count.begin(), count.end(), 0);
#define read_var_all(TYPE, DT) \
        if (type == DT) { \
            TYPE *ptr = NULL; \
            ncmpi::get_vara_all(ncid, id, &start[0], &count[0], ptr); \
        } else
        read_var_all(int,    DataType::INT)
        read_var_all(float,  DataType::FLOAT)
        read_var_all(double, DataType::DOUBLE)
        ; // for last else above
#undef read_var_all
    }

    return dst;
}


ostream& NetcdfVariable::print(ostream &os) const
{
    TIMING("NetcdfVariable::print(ostream)");
    os << "NetcdfVariable(" << name << ")";
    return os;
}


NetcdfDataset* NetcdfVariable::get_dataset() const
{
    TIMING("NetcdfVariable::get_dataset()");
    return dataset;
}


int NetcdfVariable::get_id() const
{
    TIMING("NetcdfVariable::get_id()");
    return id;
}
