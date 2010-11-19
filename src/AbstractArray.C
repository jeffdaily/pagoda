#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include "AbstractArray.H"
#include "Array.H"
#include "Timing.H"
#include "Util.H"


AbstractArray::AbstractArray()
    :   Array()
    ,   _has_fill_value(false)
    ,   _fill_value(0.0)
    ,   counter(NULL)
{
    TIMING("AbstractArray::AbstractArray()");
}


AbstractArray::AbstractArray(const AbstractArray &that)
    :   Array()
    ,   _has_fill_value(that._has_fill_value)
    ,   _fill_value(that._fill_value)
    ,   counter(that.counter)
{
    TIMING("AbstractArray::AbstractArray()");
    /** @todo should counter be deep copied? */
}


AbstractArray::~AbstractArray()
{
    TIMING("AbstractArray::~AbstractArray()");
}


int64_t AbstractArray::get_size() const
{
    TIMING("AbstractArray::get_size()");
    return pagoda::shape_to_size(get_shape());
}


int64_t AbstractArray::get_local_size() const
{
    TIMING("AbstractArray::get_local_size()");
    return pagoda::shape_to_size(get_local_shape());
}


int64_t AbstractArray::get_ndim() const
{
    return static_cast<int64_t>(get_shape().size());
}


bool AbstractArray::is_scalar() const
{
    return get_ndim() == 0;
}


void AbstractArray::fill(void *value)
{
    void *data = access();
    DataType type = get_type();

    if (NULL != data) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            T tvalue = *static_cast<T*>(value); \
            T *tdata = static_cast<T*>(data); \
            for (int64_t i=0,limit=get_local_size(); i<limit; ++i) { \
                tdata[i] = tvalue; \
            } \
        } else
#include "DataType.def"
        release_update();
    }
}


void AbstractArray::copy(const Array *src)
{
    void *data = access();

    ASSERT(same_distribution(src));
    ASSERT(get_type() == src->get_type());
    if (NULL != data) {

        release_update();
    }
}


bool AbstractArray::same_distribution(const Array *other) const
{
    vector<long> values(
            1, (get_local_shape() == other->get_local_shape()) ? 1 : 0);

    TIMING("AbstractArray::same_distribution(Array*)");

    pagoda::gop_min(values);
    return (values.at(0) == 1) ? true : false;
}


bool AbstractArray::owns_data() const
{
    return get_local_size() > 0;
}


void AbstractArray::set_fill_value(double value)
{
    _fill_value = value;
    _has_fill_value = true;
}


bool AbstractArray::has_fill_value() const
{
    return _has_fill_value;
}


double AbstractArray::get_fill_value() const
{
    return _fill_value;
}


void AbstractArray::clear_fill_value()
{
    _fill_value = 0.0;
    _has_fill_value = false;
}


void AbstractArray::set_counter(Array *counter)
{
    this->counter = counter;
}


ostream& AbstractArray::print(ostream &os) const
{
    TIMING("AbstractArray::print(ostream&)");
    os << "AbstractArray";
    return os;
}


bool AbstractArray::broadcast_check(const Array *rhs) const
{
    vector<int64_t> my_shape = get_shape();
    vector<int64_t> rhs_shape = rhs->get_shape();
    vector<int64_t>::reverse_iterator my_it;
    vector<int64_t>::reverse_iterator rhs_it;

    // this Array must have the same or greater rank
    if (my_shape.size() < rhs_shape.size()) {
        return false;
    }

    while (my_shape.size() != rhs_shape.size()) {
        my_shape.erase(my_shape.begin());
    }

    if (my_shape != rhs_shape) {
        return false;
    }

    return true;
}
