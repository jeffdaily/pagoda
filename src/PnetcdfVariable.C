#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <pnetcdf.h>

#include <algorithm>

#include "Array.H"
#include "Attribute.H"
#include "Common.H"
#include "Copy.H"
#include "Error.H"
#include "Mask.H"
#include "PnetcdfAttribute.H"
#include "PnetcdfDataset.H"
#include "PnetcdfDimension.H"
#include "PnetcdfError.H"
#include "PnetcdfVariable.H"
#include "Pack.H"
#include "PnetcdfNS.H"
#include "ScalarArray.H"
#include "Util.H"

using std::fill;


PnetcdfVariable::PnetcdfVariable(PnetcdfDataset *dataset, int varid)
    :   AbstractVariable()
    ,   dataset(dataset)
    ,   id(varid)
    ,   name("")
    ,   dims()
    ,   atts()
    ,   type(DataType::NOT_A_TYPE)
    ,   nb_requests()
    ,   nb_buffers()
    ,   nb_arrays_to_release()
    ,   nb_arrays_to_pack_src()
    ,   nb_arrays_to_pack_dst()
{
    int ncid = dataset->get_id();
    vector<int> dim_ids;
    nc_type type_tmp;
    int natt;
    ncmpi::inq_var(ncid, varid, name, type_tmp, dim_ids, natt);
    type = PnetcdfDataset::to_dt(type_tmp);
    for (size_t dimidx=0; dimidx<dim_ids.size(); ++dimidx) {
        dims.push_back(dataset->get_dim(dim_ids[dimidx]));
    }
    for (int attid=0; attid<natt; ++attid) {
        atts.push_back(new PnetcdfAttribute(dataset, attid, this));
    }
}


PnetcdfVariable::~PnetcdfVariable()
{

    transform(atts.begin(), atts.end(), atts.begin(),
              pagoda::ptr_deleter<PnetcdfAttribute*>);
}


string PnetcdfVariable::get_name() const
{
    return name;
}


vector<Dimension*> PnetcdfVariable::get_dims() const
{
    return vector<Dimension*>(dims.begin(), dims.end());
}


vector<Attribute*> PnetcdfVariable::get_atts() const
{
    return vector<Attribute*>(atts.begin(), atts.end());
}


Dataset* PnetcdfVariable::get_dataset() const
{
    return dataset;
}


DataType PnetcdfVariable::get_type() const
{
    return type;
}


#if 0
ScalarArray* PnetcdfVariable::read1(const vector<int64_t> &index,
                                    ScalarArray *dst) const
{
    int ncid = get_netcdf_dataset()->get_id();
    vector<MPI_Offset> _index(index.begin(), index.end());
    DataType type = get_type();
    void *buf;

    if (dst == NULL) {
        dst = new ScalarArray(type);
    }

    buf = dst->access();
#define read_var1(T,DT) \
    if (DT == type) { \
        T *tbuf = static_cast<T*>(buf); \
        ncmpi::get_var1(ncid, id, _index, tbuf); \
    } else
    read_var1(unsigned char, DataType::UCHAR)
    read_var1(signed char,   DataType::SCHAR)
    read_var1(char,          DataType::CHAR)
    read_var1(int,           DataType::INT)
    read_var1(long,          DataType::LONG)
    read_var1(float,         DataType::FLOAT)
    read_var1(double,        DataType::DOUBLE)
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
#undef read_var1
    dst->release_update();

    return dst;
}
#endif


Array* PnetcdfVariable::read(Array *dst) const
{
    if (dst == NULL) {
        return AbstractVariable::read_alloc();
    }
    return _read(dst);
}


Array* PnetcdfVariable::iread(Array *dst)
{
    if (dst == NULL) {
        return AbstractVariable::iread_alloc();
    }
    return _iread(dst);
}


Array* PnetcdfVariable::read(int64_t record, Array *dst) const
{
    if (dst == NULL) {
        return AbstractVariable::read_alloc(record);
    }
    return _read(record, dst);
}


Array* PnetcdfVariable::iread(int64_t record, Array *dst)
{
    if (dst == NULL) {
        return AbstractVariable::iread_alloc(record);
    }
    return _iread(record, dst);
}


Array* PnetcdfVariable::read_prep(Array *dst,
        vector<MPI_Offset> &start, vector<MPI_Offset> &count,
        bool &found_bit) const
{
    int64_t ndim = get_ndim();
    vector<int64_t> lo(ndim);
    vector<int64_t> hi(ndim);
    Array *tmp;

    // propagate fill value to Array
    if (has_validator()) {
        dst->set_validator(get_validator());
    }

    // if we are subsetting, then the passed in array is different than the
    // one in which the data is read into i.e. subset occurs after the fact
    if (needs_subset()) {
        get_dataset()->push_masks(NULL);
        tmp = Array::create(type, get_shape());
        get_dataset()->pop_masks();
    }
    else {
        tmp = dst;
    }

    tmp->get_distribution(lo,hi);

    if (tmp->get_ndim() != ndim) {
        pagoda::abort("PnetcdfVariable::read(Array*) :: shape mismatch");
    }

    found_bit = find_bit(get_dims(), lo, hi);

    if (tmp->owns_data() && found_bit) {
        for (int64_t dimidx=0; dimidx<ndim; ++dimidx) {
            start[dimidx] = lo[dimidx];
            count[dimidx] = hi[dimidx] - lo[dimidx] + 1;
        }
    }
    else {
        // make a non-participating process a no-op
        fill(start.begin(), start.end(), 0);
        fill(count.begin(), count.end(), 0);
    }

    return tmp;
}


Array* PnetcdfVariable::_read(Array *dst) const
{
    int64_t ndim = get_ndim();
    vector<MPI_Offset> start(ndim);
    vector<MPI_Offset> count(ndim);
    bool found_bit = true;
    Array *tmp;


    tmp = read_prep(dst, start, count, found_bit);
    do_read(tmp, start, count, found_bit);

    // check whether a subset is needed
    if (needs_subset()) {
        pagoda::pack(tmp, dst, get_masks());
        delete tmp;
        if (needs_renumber()) {
            renumber(dst);
        }
    }

    return dst;
}


Array* PnetcdfVariable::_iread(Array *dst)
{
    int64_t ndim = get_ndim();
    vector<MPI_Offset> start(ndim);
    vector<MPI_Offset> count(ndim);
    bool found_bit = true;
    Array *tmp;


    tmp = read_prep(dst, start, count, found_bit);
    do_iread(tmp, start, count, found_bit);

    // can't perform subset until read is finished
    if (needs_subset()) {
        nb_arrays_to_pack_dst.push_back(dst);
        nb_arrays_to_pack_src.push_back(tmp);
    } else {
        nb_arrays_to_pack_dst.push_back(NULL);
        nb_arrays_to_pack_src.push_back(NULL);
    }

    return dst;
}


Array* PnetcdfVariable::read_prep(Array *dst,
        vector<MPI_Offset> &start, vector<MPI_Offset> &count,
        bool &found_bit, int64_t record) const
{
    int64_t ndim = get_ndim();
    vector<int64_t> lo(ndim);
    vector<int64_t> hi(ndim);
    vector<Dimension*> adims(dims.begin()+1,dims.end());
    Array *tmp;

    // propagate fill value to Array
    if (has_validator(record)) {
        dst->set_validator(get_validator(record));
    }

    // if we are subsetting, then the passed in array is different than the
    // one in which the data is read into i.e. subset occurs after the fact
    if (needs_subset_record()) {
        vector<int64_t> shape;
        get_dataset()->push_masks(NULL);
        shape = get_shape();
        get_dataset()->pop_masks();
        shape.erase(shape.begin());
        tmp = Array::create(type, shape);
    }
    else {
        tmp = dst;
    }

    tmp->get_distribution(lo,hi);

    if (tmp->get_ndim()+1 != ndim) {
        pagoda::abort("PnetcdfVariable::read(int64_t,Array*) :: shape mismatch");
    }

    found_bit = find_bit(adims, lo, hi);

    if (tmp->owns_data() && found_bit) {
        if (needs_subset_record()) {
            start[0] = translate_record(record);
        }
        else {
            start[0] = record;
        }
        count[0] = 1;
        for (int64_t dimidx=1; dimidx<ndim; ++dimidx) {
            start[dimidx] = lo[dimidx-1];
            count[dimidx] = hi[dimidx-1] - lo[dimidx-1] + 1;
        }
    }
    else {
        // make a non-participating process a no-op
        fill(start.begin(), start.end(), 0);
        fill(count.begin(), count.end(), 0);
    }

    return tmp;
}


Array* PnetcdfVariable::_read(int64_t record, Array *dst) const
{
    int64_t ndim = get_ndim();
    vector<MPI_Offset> start(ndim);
    vector<MPI_Offset> count(ndim);
    bool found_bit = true;
    Array *tmp;


    tmp = read_prep(dst, start, count, found_bit, record);
    do_read(tmp, start, count, found_bit);

    // check whether a subset is needed
    if (needs_subset_record()) {
        vector<Mask*> masks = get_masks();
        masks.erase(masks.begin());
        pagoda::pack(tmp, dst, masks);
        delete tmp;
        if (needs_renumber()) {
            renumber(dst);
        }
    }

    return dst;
}


Array* PnetcdfVariable::_iread(int64_t record, Array *dst)
{
    int64_t ndim = get_ndim();
    vector<MPI_Offset> start(ndim);
    vector<MPI_Offset> count(ndim);
    bool found_bit = true;
    Array *tmp;


    tmp = read_prep(dst, start, count, found_bit, record);
    do_iread(tmp, start, count, found_bit);

    // can't perform subset until read is finished
    if (needs_subset_record()) {
        nb_arrays_to_pack_dst.push_back(dst);
        nb_arrays_to_pack_src.push_back(tmp);
    } else {
        nb_arrays_to_pack_dst.push_back(NULL);
        nb_arrays_to_pack_src.push_back(NULL);
    }

    return dst;
}


bool PnetcdfVariable::find_bit(const vector<Dimension*> &adims,
                               const vector<int64_t> &lo,
                               const vector<int64_t> &hi) const
{
    bool found_bit = true;

#ifdef READ_OPT
    // only attempt the optimization for record variables
    // **Why did I decide to do it that way??**
    // check all dimension masks for a non-zero bit

    int64_t ndim = adims.size();

    for (int64_t i=0; i<ndim; ++i) {
        bool current_found_bit = false;
        Mask *mask = get_dataset()->get_masks()->get(adims[i]);
        if (mask) {
            int *data = mask->get(lo[i],hi[i]);

            for (int64_t j=0,limit=hi[i]-lo[i]+1; j<limit; ++j) {
                if (data[j] != 0) {
                    current_found_bit = true;
                    break;
                }
            }
            found_bit &= current_found_bit;
            delete [] data;
        }
    }
#endif

    return found_bit;
}


void PnetcdfVariable::do_read(Array *dst, const vector<MPI_Offset> &start,
                              const vector<MPI_Offset> &count,
                              bool found_bit) const
{
    int ncid = get_netcdf_dataset()->get_id();
    const DataType read_type = dst->get_read_type();
    const DataType type = dst->get_type();

#define read_var_all(TYPE, DT) \
    if (read_type == DT) { \
        TYPE *ptr = NULL; \
        int64_t n = dst->get_local_size(); \
        if (dst->owns_data() && found_bit) { \
            if (read_type == type) { \
                ptr = static_cast<TYPE*>(dst->access()); \
            } else { \
                ptr = new TYPE[n]; \
            } \
        } \
        ncmpi::get_vara_all(ncid, id, start, count, ptr); \
        if (dst->owns_data() && found_bit) { \
            if (read_type != type) { \
                void *gabuf = NULL; \
                gabuf = dst->access(); \
                pagoda::copy(read_type,ptr,type,gabuf,n); \
                delete [] ptr; \
            } \
            dst->release_update(); \
        } \
    } else
    read_var_all(unsigned char, DataType::UCHAR)
    read_var_all(signed char,   DataType::SCHAR)
    read_var_all(char,          DataType::CHAR)
    read_var_all(int,           DataType::INT)
    read_var_all(long,          DataType::LONG)
    read_var_all(float,         DataType::FLOAT)
    read_var_all(double,        DataType::DOUBLE)
    {
        EXCEPT(DataTypeException, "DataType not handled", read_type);
    }
#undef read_var_all
}


void PnetcdfVariable::do_iread(Array *dst, const vector<MPI_Offset> &start,
                               const vector<MPI_Offset> &count,
                               bool found_bit)
{
    int ncid = get_netcdf_dataset()->get_id();
    const DataType read_type = dst->get_read_type();
    const DataType type = dst->get_type();

#define read_var_all(TYPE, DT) \
    if (read_type == DT) { \
        int request; \
        TYPE *ptr = NULL; \
        if (dst->owns_data() && found_bit) { \
            int64_t n = dst->get_local_size(); \
            nb_arrays_to_release.push_back(dst); \
            if (read_type == type) { \
                ptr = static_cast<TYPE*>(dst->access()); \
                nb_buffers.push_back(NULL); \
            } else {\
                ptr = new TYPE[n]; \
                nb_buffers.push_back(ptr); \
            } \
        } else { \
            nb_buffers.push_back(NULL); \
            nb_arrays_to_release.push_back(NULL); \
        } \
        request = ncmpi::iget_vara(ncid, id, start, count, ptr); \
        nb_requests.push_back(request); \
    } else
    read_var_all(unsigned char, DataType::UCHAR)
    read_var_all(signed char,   DataType::SCHAR)
    read_var_all(char,          DataType::CHAR)
    read_var_all(int,           DataType::INT)
    read_var_all(long,          DataType::LONG)
    read_var_all(float,         DataType::FLOAT)
    read_var_all(double,        DataType::DOUBLE)
    {
        EXCEPT(DataTypeException, "DataType not handled", read_type);
    }
#undef read_var_all
}


void PnetcdfVariable::after_wait()
{
    for (size_t i=0; i<nb_requests.size(); ++i) {
        if (NULL != nb_buffers[i]) {
            // the read type differed from the stored type
            void *buf = nb_buffers[i];
            Array *dst = nb_arrays_to_release[i];
            void *gabuf = dst->access();
            DataType type = dst->get_read_type();
            DataType ga_type = dst->get_type();
            int64_t n = dst->get_local_size(); \
            pagoda::copy(type,buf,ga_type,gabuf,n);
            dst->release_update();
#define DATATYPE_EXPAND(DT,T) \
            if (DT == type) { \
                T *tbuf = static_cast<T*>(buf); \
                delete [] tbuf; \
                tbuf = NULL; \
            } else
#include "DataType.def"
            {
                EXCEPT(DataTypeException, "DataType not handled", type);
            }
        } else if (NULL != nb_arrays_to_release[i]) {
            // read type same as stored type, simple release
            Array *dst = nb_arrays_to_release[i];
            dst->release_update();
        }

        // pack if needed
        if (NULL != nb_arrays_to_pack_dst[i]) {
            Array *dst = nb_arrays_to_pack_dst[i];
            Array *src = nb_arrays_to_pack_src[i];
            ASSERT(NULL != src);
            vector<Mask*> masks = get_masks();
            if (int64_t(masks.size()) == (src->get_ndim()+1)) {
                // assume this was a record subset
                masks.erase(masks.begin());
            }
            pagoda::pack(src, dst, masks);
            if (needs_renumber()) {
                renumber(dst);
            }
            delete src;
        }
    }

    nb_arrays_to_release.clear();
    nb_arrays_to_pack_dst.clear();
    nb_arrays_to_pack_src.clear();
    nb_buffers.clear();
    nb_requests.clear();
}


bool PnetcdfVariable::needs_renumber() const
{
    return AbstractVariable::needs_renumber();
}


void PnetcdfVariable::renumber(Array *array) const
{
    AbstractVariable::renumber(array);
}


ostream& PnetcdfVariable::print(ostream &os) const
{
    os << "PnetcdfVariable(" << name << ")";
    return os;
}


PnetcdfDataset* PnetcdfVariable::get_netcdf_dataset() const
{
    return dataset;
}


int PnetcdfVariable::get_id() const
{
    return id;
}
