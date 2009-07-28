#include <sstream>
    using std::istringstream;
#include <stdlib.h>
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "LatLonBox.H"
#include "RangeException.H"


const LatLonBox LatLonBox::GLOBAL(90,-90,180,-180);


LatLonBox::LatLonBox()
{
    *this = GLOBAL;
}


LatLonBox::LatLonBox(const string &latLonString)
{
    set(latLonString);
}


LatLonBox::LatLonBox(
        const double &north,
        const double &south,
        const double &east,
        const double &west)
{
    set(north,south,east,west);
}


LatLonBox::LatLonBox(const LatLonBox &range)
    :   n(range.n),
        s(range.s),
        e(range.e),
        w(range.w),
        x(range.x),
        y(range.y)
{
}


void LatLonBox::set(const string &latLonString)
{
    vector<string> latLonParts;
    istringstream ss(latLonString);
    string token;
    while (!ss.eof()) {
        getline(ss, token, ',');
        latLonParts.push_back(token);
    }

    if (latLonParts.size() != 4) {
        throw RangeException("invalid box string");
    }

    n = strtol(latLonParts[0].c_str(), NULL, 10);
    s = strtol(latLonParts[1].c_str(), NULL, 10);
    e = strtol(latLonParts[2].c_str(), NULL, 10);
    w = strtol(latLonParts[3].c_str(), NULL, 10);
    x = ((w+e)/2);
    y = ((n+s)/2);

    check();
}


void LatLonBox::set(
        const double &north,
        const double &south,
        const double &east,
        const double &west)
{
    n = north;
    s = south;
    e = east;
    w = west;
    x = (w+e)/2;
    y = (n+s)/2;
    check();
}



bool LatLonBox::operator == (const LatLonBox &that) const
{
    return this->n == that.n
        && this->s == that.s
        && this->e == that.e
        && this->w == that.w;
}



bool LatLonBox::operator != (const LatLonBox &that) const
{
    return !(*this == that);
}



bool LatLonBox::operator <  (const LatLonBox &that) const
{
    return this->n < that.n
        && this->s > that.s
        && this->e < that.e
        && this->w > that.w;
}



bool LatLonBox::operator <= (const LatLonBox &that) const
{
    return (*this < that) || (*this == that);
}



bool LatLonBox::operator >  (const LatLonBox &that) const
{
    return !(*this <= that);
}



bool LatLonBox::operator >= (const LatLonBox &that) const
{
    return !(*this < that);
}



bool LatLonBox::contains(const double &lat, const double &lon) const
{
    return s<lat && lat<n && w<lon && lon<e;
}


void LatLonBox::scale(const double &value)
{
    n *= value;
    s *= value;
    e *= value;
    w *= value;
}


LatLonBox LatLonBox::enclose(const LatLonBox &first, const LatLonBox &second)
{
    LatLonBox box;
    box.n = first.n > second.n ? first.n : second.n;
    box.s = first.s < second.s ? first.s : second.s;
    box.e = first.e > second.e ? first.e : second.e;
    box.w = first.w < second.w ? first.w : second.w;
    return box;
}


ostream& operator<<(ostream &os, const LatLonBox &box)
{
    return os
        << box.n << ","
        << box.s << ","
        << box.e << ","
        << box.w << ","
        << box.x << ","
        << box.y;
}


void LatLonBox::check()
{
    if (n > 90 || n < -90
            || s > 90 || s < -90
            || e > 180 || e < -180
            || w > 180 || w < -180)
        throw RangeException("");
}

