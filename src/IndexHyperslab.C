#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "IndexHyperslab.H"
#include "RangeException.H"

using std::istringstream;
using std::ostream;
using std::string;
using std::vector;


IndexHyperslab::IndexHyperslab(const string &name, int64_t index)
    :   name(name)
    ,   min(index)
    ,   max(0)
    ,   stride(0)
    ,   min_is_set(true)
    ,   max_is_set(false)
    ,   stride_is_set(false)
{
}


IndexHyperslab::IndexHyperslab(const string &name, int64_t min, int64_t max)
    :   name(name)
    ,   min(min)
    ,   max(max)
    ,   stride(0)
    ,   min_is_set(true)
    ,   max_is_set(true)
    ,   stride_is_set(false)
{
}


IndexHyperslab::IndexHyperslab(const string &name,
                               int64_t min, int64_t max, int64_t stride)
    :   name(name)
    ,   min(min)
    ,   max(max)
    ,   stride(stride)
    ,   min_is_set(true)
    ,   max_is_set(true)
    ,   stride_is_set(true)
{
}


IndexHyperslab::IndexHyperslab(string arg)
    :   name("")
    ,   min(0)
    ,   max(0)
    ,   stride(0)
    ,   min_is_set(false)
    ,   max_is_set(false)
    ,   stride_is_set(false)
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
    else if (parts.size() == 4) {
        name = parts[0];
        if (parts[1].empty() && parts[2].empty() && parts[3].empty()) {
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
        if (!parts[3].empty()) {
            istringstream pss(parts[3]);
            pss >> stride;
            stride_is_set = true;
            if (pss.fail()) {
                throw RangeException("invalid dimension string");
            }
        }
    }
    else {
        throw RangeException("invalid dimension string");
    }
}


IndexHyperslab::IndexHyperslab(const IndexHyperslab &that)
    :   name(that.name)
    ,   min(that.min)
    ,   max(that.max)
    ,   stride(that.stride)
    ,   min_is_set(that.min_is_set)
    ,   max_is_set(that.max_is_set)
    ,   stride_is_set(that.stride_is_set)
{
}


IndexHyperslab::~IndexHyperslab()
{
}


string IndexHyperslab::get_name() const
{
    return name;
}


int64_t IndexHyperslab::get_min() const
{
    return min;
}


int64_t IndexHyperslab::get_max() const
{
    return max;
}


int64_t IndexHyperslab::get_stride() const
{
    return stride;
}


bool IndexHyperslab::has_min() const
{
    return min_is_set;
}


bool IndexHyperslab::has_max() const
{
    return max_is_set;
}


bool IndexHyperslab::has_stride() const
{
    return stride_is_set;
}


bool IndexHyperslab::operator== (const IndexHyperslab &that) const
{
    return name == that.name
        && min == that.min
        && max == that.max
        && stride == that.stride
        && min_is_set == that.min_is_set
        && max_is_set == that.max_is_set
        && stride_is_set == that.stride_is_set;
}


IndexHyperslab& IndexHyperslab::operator= (const IndexHyperslab &that)
{
    if (this == &that) {
        return *this;
    }

    name = that.name;
    min = that.min;
    max = that.max;
    stride = that.stride;
    min_is_set = that.min_is_set;
    max_is_set = that.max_is_set;
    stride_is_set = that.stride_is_set;

    return *this;
}


ostream& operator<< (ostream &os, const IndexHyperslab &hyperslab)
{
    os << "IndexHyperslab(";
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
    os << ",";
    if (hyperslab.has_stride()) {
        os << hyperslab.get_stride();
    } else {
        os << "_";
    }
    os << ")";
    return os;
}

