#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cmath>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include "LatLonBox.H"
#include "RangeException.H"
#include "Timing.H"

using std::ceil;
using std::floor;
using std::istringstream;
using std::string;
using std::vector;


const LatLonBox LatLonBox::GLOBAL(90.0,-90.0,180.0,-180.0);


LatLonBox::LatLonBox()
{
    TIMING("LatLonBox::LatLonBox()");
    *this = GLOBAL;
}


LatLonBox::LatLonBox(const string &latLonString)
{
    TIMING("LatLonBox::LatLonBox(string)");
    set(latLonString);
}


LatLonBox::LatLonBox(double north, double south, double east, double west)
{
    TIMING("LatLonBox::LatLonBox(double,double,double,double)");
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
    TIMING("LatLonBox::LatLonBox(LatLonBox)");
}


void LatLonBox::set(const string &latLonString)
{
    TIMING("LatLonBox::set(string)");
    vector<string> latLonParts;
    istringstream iss(latLonString);

    string token;
    while (!iss.eof()) {
        getline(iss, token, ',');
        latLonParts.push_back(token);
    }

    if (latLonParts.size() != 4) {
        throw RangeException(string("invalid box string"));
    }

    istringstream sn(latLonParts[0]);
    istringstream ss(latLonParts[1]);
    istringstream se(latLonParts[2]);
    istringstream sw(latLonParts[3]);
    sn >> n;
    ss >> s;
    se >> e;
    sw >> w;
    if (!sn || !ss || !se || !sw) {
        throw RangeException(string("invalid box string"));
    }
    x = ((w+e)/2);
    y = ((n+s)/2);

    check();
}


void LatLonBox::set(double north, double south, double east, double west)
{
    TIMING("LatLonBox::set(double,double,double,double)");
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
    TIMING("LatLonBox::operator==(LatLonBox)");
    return this->n == that.n
        && this->s == that.s
        && this->e == that.e
        && this->w == that.w;
}



bool LatLonBox::operator != (const LatLonBox &that) const
{
    TIMING("LatLonBox::operator!=(LatLonBox)");
    return !(*this == that);
}



bool LatLonBox::operator <  (const LatLonBox &that) const
{
    TIMING("LatLonBox::operator<(LatLonBox)");
    return this->n < that.n
        && this->s > that.s
        && this->e < that.e
        && this->w > that.w;
}



bool LatLonBox::operator <= (const LatLonBox &that) const
{
    TIMING("LatLonBox::operator<=(LatLonBox)");
    return (*this < that) || (*this == that);
}



bool LatLonBox::operator >  (const LatLonBox &that) const
{
    TIMING("LatLonBox::operator>(LatLonBox)");
    return !(*this <= that);
}



bool LatLonBox::operator >= (const LatLonBox &that) const
{
    TIMING("LatLonBox::operator>=(LatLonBox)");
    return !(*this < that);
}



bool LatLonBox::contains(int lat, int lon) const
{
    TIMING("LatLonBox::contains(int,int)");
    long long llat = lat;
    long long llon = lon;
    return contains(llat, llon);
}


bool LatLonBox::contains(long lat, long lon) const
{
    TIMING("LatLonBox::contains(long,long)");
    long long llat = lat;
    long long llon = lon;
    return contains(llat, llon);
}


bool LatLonBox::contains(long long lat, long long lon) const
{
    TIMING("LatLonBox::contains(long long,long long)");
    long long _n = (0 < n) ? floor(n) : ceil(n);
    long long _s = (0 < s) ? floor(s) : ceil(s);
    long long _e = (0 < e) ? floor(e) : ceil(e);
    long long _w = (0 < w) ? floor(w) : ceil(w);
    return _s<lat && lat<_n && _w<lon && lon<_e;
}


bool LatLonBox::contains(float lat, float lon) const
{
    TIMING("LatLonBox::contains(float,float)");
    double dlat = lat;
    double dlon = lon;
    return contains(dlat, dlon);
}


bool LatLonBox::contains(double lat, double lon) const
{
    TIMING("LatLonBox::contains(double,double)");
    return s<lat && lat<n && w<lon && lon<e;
}


void LatLonBox::scale(double value)
{
    TIMING("LatLonBox::scale(double)");
    n *= value;
    s *= value;
    e *= value;
    w *= value;
}


LatLonBox LatLonBox::enclose(const LatLonBox &first, const LatLonBox &second)
{
    TIMING("LatLonBox::enclose(LatLonBox,LatLonBox)");
    LatLonBox box;
    box.n = first.n > second.n ? first.n : second.n;
    box.s = first.s < second.s ? first.s : second.s;
    box.e = first.e > second.e ? first.e : second.e;
    box.w = first.w < second.w ? first.w : second.w;
    return box;
}


ostream& operator<<(ostream &os, const LatLonBox &box)
{
    TIMING("operator<<(ostream,LatLonBox)");
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
    TIMING("LatLonBox::check()");
    if (n > 90 || n < -90
            || s > 90 || s < -90
            || e > 180 || e < -180
            || w > 180 || w < -180)
        throw RangeException(string(""));
}
