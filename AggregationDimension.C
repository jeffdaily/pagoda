#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <string>
#include <vector>

#include "AggregationDimension.H"
#include "Dimension.H"

using std::string;
using std::vector;


AggregationDimension::AggregationDimension(Dimension *dim)
    :   Dimension()
    ,   dims(1, dim)
{
}


AggregationDimension::~AggregationDimension()
{
}


void AggregationDimension::add(Dimension *dim)
{
    dims.push_back(dim);
}


string AggregationDimension::get_name() const
{
    return dims.at(0)->get_name();
}


int64_t AggregationDimension::get_size() const
{
    int64_t size=0;
    for (size_t idx=0,limit=dims.size(); idx<limit; ++idx) {
        size += dims.at(idx)->get_size();
    }
    return size;
}


bool AggregationDimension::is_unlimited() const
{
    return dims.at(0)->is_unlimited();
}


ostream& AggregationDimension::print(ostream &os) const
{
    os << "AggregationDimension(";
    os << get_name() << ",";
    os << get_size() << ",";
    os << is_unlimited() << ")";
    return os;
}
