#include <algorithm>
    using std::transform;
#include <cctype>
    using std::tolower;

#include "StringComparator.H"


StringComparator::StringComparator()
    :   value("")
    ,   ignore_case(true)
    ,   within(false)
{
}


StringComparator::StringComparator(
        const string &value,
        bool ignore_case,
        bool within)
    :   value(value)
    ,   ignore_case(ignore_case)
    ,   within(within)
{
}


StringComparator::StringComparator(const StringComparator &copy)
    :   value(copy.value)
    ,   ignore_case(copy.ignore_case)
    ,   within(copy.within)
{
}


StringComparator& StringComparator::operator = (const StringComparator &copy)
{
    if (&copy != this) {
        value = copy.value;
        ignore_case = copy.ignore_case;
        within = copy.within;
    }
    return *this;
}


StringComparator::~StringComparator()
{
}


bool StringComparator::operator () (const string &that) const
{
    string mine = value;
    string theirs = that;
    if (ignore_case) {
        transform(mine.begin(), mine.end(), mine.begin(), tolower);
        transform(theirs.begin(), theirs.end(), theirs.begin(), tolower);
    }
    if (within) {
        return theirs.find(mine) != string::npos;
    } else {
        return mine == theirs;
    }
}


void StringComparator::set_value(const string &value)
{
    this->value = value;
}


void StringComparator::set_ignore_case(bool ignore_case)
{
    this->ignore_case = ignore_case;
}


void StringComparator::set_within(bool within)
{
    this->within = within;
}


string StringComparator::get_value() const
{
    return value;
}


bool StringComparator::get_ignore_case() const
{
    return ignore_case;
}


bool StringComparator::get_within() const
{
    return within;
}

