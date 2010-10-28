#ifndef LATLONBOX_H_
#define LATLONBOX_H_

#include <iostream>
#include <string>

using std::string;
using std::ostream;


/**
 * Represents a rectangular latitude/longitude region.
 */
class LatLonBox
{
    public:
        LatLonBox();
        LatLonBox(const string &latLonString, bool aux_order=false);
        LatLonBox(double north, double south, double east, double west);
        LatLonBox(const LatLonBox &range);

        void set(const string &latLonString, bool aux_order=false);
        void set(double north, double south, double east, double west);

        bool operator == (const LatLonBox &that) const;
        bool operator != (const LatLonBox &that) const;
        bool operator < (const LatLonBox &that) const;
        bool operator <= (const LatLonBox &that) const;
        bool operator > (const LatLonBox &that) const;
        bool operator >= (const LatLonBox &that) const;

        bool contains(int lat, int lon) const;
        bool contains(long lat, long lon) const;
        bool contains(long long lat, long long lon) const;
        bool contains(float lat, float lon) const;
        bool contains(double lat, double lon) const;
        bool contains(long double lat, long double lon) const;
        void scale(double value);

        LatLonBox& operator*=(double value);
        LatLonBox  operator*(double value) const;

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
