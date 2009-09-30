#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <string>
#include <vector>

#include "AggregationDimension.H"
#include "Dimension.H"
#include "Timing.H"

using std::string;
using std::vector;


AggregationDimension::AggregationDimension(Dimension *dim)
    :   Dimension()
    ,   name(dim->get_name())
    ,   size(dim->get_size())
    ,   _is_unlimited(dim->is_unlimited())
{
    TIMING("AggregationDimension::AggregationDimension(Dimension*)");
}


AggregationDimension::~AggregationDimension()
{
    TIMING("AggregationDimension::~AggregationDimension()");
}


void AggregationDimension::add(Dimension *dim)
{
    TIMING("AggregationDimension::add(Dimension*)");
    size += dim->get_size();
}


string AggregationDimension::get_name() const
{
    TIMING("AggregationDimension::get_name()");
    return name;
}


int64_t AggregationDimension::get_size() const
{
    TIMING("AggregationDimension::get_size()");
    return size;
}


bool AggregationDimension::is_unlimited() const
{
    TIMING("AggregationDimension::is_unlimited()");
    return _is_unlimited;
}


ostream& AggregationDimension::print(ostream &os) const
{
    TIMING("AggregationDimension::print(ostream)");
    os << "AggregationDimension(";
    os << get_name() << ",";
    os << get_size() << ",";
    os << is_unlimited() << ")";
    return os;
}
