#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "CoordHyperslab.H"
#include "RangeException.H"

using std::istringstream;
using std::ostream;
using std::string;
using std::vector;


CoordHyperslab::CoordHyperslab(const string &name, double min, double max)
    :   name(name)
    ,   min(min)
    ,   max(max)
    ,   min_is_set(true)
    ,   max_is_set(true)
{
}


CoordHyperslab::CoordHyperslab(string arg)
    :   name("")
    ,   min(0)
    ,   max(0)
    ,   min_is_set(false)
    ,   max_is_set(false)
{
    vector<string> parts;
    istringstream ss(arg);
    string token;
    while (!ss.eof()) {
        getline(ss, token, ',');
        parts.push_back(token);
    }

    if (parts.size() == 2) {
        name = parts[0];
        if (parts[1].empty()) {
            throw RangeException("invalid dimension string");
        } else {
            istringstream pss(parts[1]);
            pss >> min;
            min_is_set = true;
            if (pss.fail()) {
                throw RangeException("invalid dimension string");
            }
        }
    }
    else if (parts.size() == 3) {
        name = parts[0];
        if (parts[1].empty() && parts[2].empty()) {
            throw RangeException("invalid dimension string");
        }
        if (!parts[1].empty()) {
            istringstream pss(parts[1]);
            pss >> min;
            min_is_set = true;
            if (pss.fail()) {
                throw RangeException("invalid dimension string");
            }
        }
        if (!parts[2].empty()) {
            istringstream pss(parts[2]);
            pss >> max;
            max_is_set = true;
            if (pss.fail()) {
                throw RangeException("invalid dimension string");
            }
        }
    }
    else {
        throw RangeException("invalid dimension string");
    }
}


CoordHyperslab::CoordHyperslab(const CoordHyperslab &that)
    :   name(that.name)
    ,   min(that.min)
    ,   max(that.max)
    ,   min_is_set(that.min_is_set)
    ,   max_is_set(that.max_is_set)
{
}


CoordHyperslab::~CoordHyperslab()
{
}


string CoordHyperslab::get_name() const
{
    return name;
}


double CoordHyperslab::get_min() const
{
    return min;
}


double CoordHyperslab::get_max() const
{
    return max;
}


bool CoordHyperslab::has_min() const
{
    return min_is_set;
}


bool CoordHyperslab::has_max() const
{
    return max_is_set;
}


bool CoordHyperslab::operator== (const CoordHyperslab &that) const
{
    return name == that.name
        && min == that.min
        && max == that.max
        && min_is_set == that.min_is_set
        && max_is_set == that.max_is_set;
}


CoordHyperslab& CoordHyperslab::operator= (const CoordHyperslab &that)
{
    if (this == &that) {
        return *this;
    }

    name = that.name;
    min = that.min;
    max = that.max;
    min_is_set = that.min_is_set;
    max_is_set = that.max_is_set;

    return *this;
}


ostream& operator<< (ostream &os, const CoordHyperslab &hyperslab)
{
    os << "CoordHyperslab(";
    if (hyperslab.has_min()) {
        os << hyperslab.get_min();
    } else {
        os << "_";
    }
    os << ",";
    if (hyperslab.has_max()) {
        os << hyperslab.get_max();
    } else {
        os << "_";
    }
    os << ")";
    return os;
}

