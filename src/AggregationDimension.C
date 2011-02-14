#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <string>
#include <vector>

#include "AggregationDimension.H"
#include "Dimension.H"
#include "Timing.H"

using std::string;
using std::vector;


AggregationDimension::AggregationDimension(Aggregation *agg, Dimension *dim)
    :   Dimension()
    ,   name(dim->get_name())
    ,   size(dim->get_size())
    ,   _is_unlimited(dim->is_unlimited())
    ,   agg(agg)
{
}


AggregationDimension::~AggregationDimension()
{
}


void AggregationDimension::add(Dimension *dim)
{
    size += dim->get_size();
}


string AggregationDimension::get_name() const
{
    return name;
}


int64_t AggregationDimension::get_size() const
{
    return size;
}


bool AggregationDimension::is_unlimited() const
{
    return _is_unlimited;
}


Dataset* AggregationDimension::get_dataset() const
{
    return (Dataset*)agg;
}


ostream& AggregationDimension::print(ostream &os) const
{
    os << "AggregationDimension(";
    os << get_name() << ",";
    os << get_size() << ",";
    os << is_unlimited() << ")";
    return os;
}
