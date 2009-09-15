#ifndef DATATYPE_H_
#define DATATYPE_H_

#include <iostream>
#include <stdexcept>
#include <string>

#include <macdecls.h>
#if HAVE_PNETCDF_H
#   include <pnetcdf.h>
#endif
#if HAVE_NETCDF_H
#   include <netcdf.h>
#endif

using std::ostream;
using std::runtime_error;
using std::string;


class DataTypeException : public runtime_error
{
    public:
        explicit DataTypeException(const string &what)
            :   runtime_error(what)
        {}
};


class DataType
{
    enum dt_type {
        dt_char=-1000, /* something unique between netcdf and GA */
        dt_short,
        dt_int,
        dt_long,
        dt_longlong,
        dt_float,
        dt_double,
        dt_longdouble,
        dt_uchar,
        dt_ushort,
        dt_uint,
        dt_ulong,
        dt_ulonglong,
        dt_string
    };

    public:
        DataType(nc_type type);
        DataType(int type);
        DataType(const DataType &type);

        DataType& operator = (nc_type type);
        DataType& operator = (int type);
        DataType& operator = (const DataType &type);
        operator nc_type () const;
        operator int () const;
        nc_type as_nc() const;
        int as_ma() const;

        bool operator == (nc_type) const;
        bool operator == (int) const;
        bool operator == (const DataType &type) const;

        friend ostream& operator << (ostream &os, const DataType &other);

        static dt_type to_dt(nc_type type);
        static dt_type to_dt(int type);
        static nc_type to_nc(dt_type type);
        static int to_ma(dt_type type);

    protected:
        dt_type value;
};

#endif // DATATYPE_H_
