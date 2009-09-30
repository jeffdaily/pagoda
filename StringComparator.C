#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <cctype>

#include "StringComparator.H"
#include "Timing.H"

using std::tolower;
using std::transform;


StringComparator::StringComparator()
    :   value("")
    ,   ignore_case(true)
    ,   within(false)
{
    TIMING("StringComparator::StringComparator()");
}


StringComparator::StringComparator(
        const string &value,
        bool ignore_case,
        bool within)
    :   value(value)
    ,   ignore_case(ignore_case)
    ,   within(within)
{
    TIMING("StringComparator::StringComparator(...)");
}


StringComparator::StringComparator(const StringComparator &copy)
    :   value(copy.value)
    ,   ignore_case(copy.ignore_case)
    ,   within(copy.within)
{
    TIMING("StringComparator::StringComparator(StringComparator)");
}


StringComparator& StringComparator::operator= (const StringComparator &copy)
{
    TIMING("StringComparator::operator=(StringComparator)");
    if (&copy != this) {
        value = copy.value;
        ignore_case = copy.ignore_case;
        within = copy.within;
    }
    return *this;
}


StringComparator::~StringComparator()
{
    TIMING("StringComparator::~StringComparator()");
}


bool StringComparator::operator() (const string &that) const
{
    TIMING("StringComparator::operator()(string)");
    string mine = value;
    string theirs = that;
    if (ignore_case) {
        transform(mine.begin(), mine.end(), mine.begin(), (int(*)(int))tolower);
        transform(theirs.begin(), theirs.end(), theirs.begin(), (int(*)(int))tolower);
    }
    if (within) {
        return mine.find(theirs) != string::npos;
    } else {
        return mine == theirs;
    }
}


/**
 * Return true if any of strings in input vector match our value.
 */
bool StringComparator::operator() (const vector<string> &that) const
{
    TIMING("StringComparator::operator()(vector<string>)");
    vector<string>::const_iterator it = that.begin();
    vector<string>::const_iterator end = that.end();

    for (; it!=end; ++it) {
        if (this->operator() (*it)) {
            return true;
        }
    }

    return false;
}


void StringComparator::set_value(const string &value)
{
    TIMING("StringComparator::set_value(string)");
    this->value = value;
}


void StringComparator::set_ignore_case(bool ignore_case)
{
    TIMING("StringComparator::set_ignore_case(bool)");
    this->ignore_case = ignore_case;
}


void StringComparator::set_within(bool within)
{
    TIMING("StringComparator::set_within(bool)");
    this->within = within;
}


string StringComparator::get_value() const
{
    TIMING("StringComparator::get_value()");
    return value;
}


bool StringComparator::get_ignore_case() const
{
    TIMING("StringComparator::get_ignore_case()");
    return ignore_case;
}


bool StringComparator::get_within() const
{
    TIMING("StringComparator::get_within()");
    return within;
}
