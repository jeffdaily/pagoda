#ifndef LATLONRANGE_H_
#define LATLONRANGE_H_

#include <iostream>
  using std::ostream;

class LatLonRange
{
  public:
    LatLonRange();
    LatLonRange(const string& latLonString);
    LatLonRange(const double& north,
                const double& south,
                const double& east,
                const double& west);
    LatLonRange(const LatLonRange& range);

    bool operator == (const LatLonRange& that) const;
    bool operator != (const LatLonRange& that) const;
    bool operator <  (const LatLonRange& that) const;
    bool operator <= (const LatLonRange& that) const;
    bool operator >  (const LatLonRange& that) const;
    bool operator >= (const LatLonRange& that) const;

    bool contains(const double& lat, const double& lon);

    static LatLonRange enclose(const LatLonRange& first,
                               const LatLonRange& second);

    void scale(const double& value);

    double n;
    double s;
    double e;
    double w;
    double x;
    double y;

    static const LatLonRange GLOBAL;

    friend ostream& operator<<(ostream& os, const LatLonRange& box);

  protected:
    void rangeCheck();
};

#endif // LATLONRANGE_H_
