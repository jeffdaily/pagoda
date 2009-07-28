#ifndef LATLONBOX_H_
#define LATLONBOX_H_

#include <iostream>
  using std::ostream;

class LatLonBox
{
  public:
    LatLonBox();
    LatLonBox(const string &latLonString);
    LatLonBox(const double &north,
              const double &south,
              const double &east,
              const double &west);
    LatLonBox(const LatLonBox &range);

    void set(const string &latLonString);
    void set(const double &north,
             const double &south,
             const double &east,
             const double &west);

    bool operator == (const LatLonBox &that) const;
    bool operator != (const LatLonBox &that) const;
    bool operator <  (const LatLonBox &that) const;
    bool operator <= (const LatLonBox &that) const;
    bool operator >  (const LatLonBox &that) const;
    bool operator >= (const LatLonBox &that) const;

    bool contains(const double &lat, const double &lon) const;
    void scale(const double &value);

    static LatLonBox enclose(const LatLonBox &first, const LatLonBox &second);

    double n;
    double s;
    double e;
    double w;
    double x;
    double y;

    static const LatLonBox GLOBAL;

    friend ostream& operator<<(ostream &os, const LatLonBox &box);

  protected:
    void check();
};

#endif // LATLONBOX_H_
