#ifndef DATATYPE_H_
#define DATATYPE_H_

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#if HAVE_PNETCDF
#   include <pnetcdf.h>
#elif HAVE_NETCDF
#   include <netcdf.h>
#endif

#include "Util.H"

using std::ostringstream;
using std::ostream;
using std::runtime_error;
using std::string;


/**
 * A common DataType object for translating from various library-specific
 * types.
 *
 * For example, between Global Arrays and netcdf.
 * Implemented as a type-safe enumeration.  There can be multiple instances of
 * a given DataType but there are a fixed set of values that the instances may
 * represent.
 */
class DataType
{
    public:
        static const DataType CHAR;
        static const DataType SHORT;
        static const DataType INT;
        static const DataType LONG;
        static const DataType LONGLONG;
        static const DataType FLOAT;
        static const DataType DOUBLE;
        static const DataType LONGDOUBLE;
        static const DataType UCHAR;
        static const DataType USHORT;
        static const DataType UINT;
        static const DataType ULONG;
        static const DataType ULONGLONG;
        static const DataType SCHAR;
        static const DataType STRING;

        DataType(int type);
        DataType(const DataType &type);
        DataType& operator = (int type);
        DataType& operator = (const DataType &type);

        bool operator == (const DataType &type) const;
        bool operator != (const DataType &type) const;

        friend ostream& operator << (ostream &os, const DataType &type);

        int to_ga() const;
        nc_type to_nc() const;
        int get_id() const {
            return id;
        }
        string get_name() const {
            return name;
        }

        template <class T> static DataType ctype();

    protected:
        static DataType to_dt(int type);
        DataType();
        DataType(const string &name);

        static int next_id;
        int id;
        string name;
};


class DataTypeException : public runtime_error
{
    public:
        explicit DataTypeException(const string &what)
            :   runtime_error(what)
        {}
        explicit DataTypeException(const string &what, int value)
            :   runtime_error(what
                              +string(" :: ")+pagoda::to_string(value))
        {}
        explicit DataTypeException(const string &what, int value,
                                   const char *file, int line)
            :   runtime_error(what
                              +string(" :: ")+pagoda::to_string(value)
                              +string(" :: ")+string(file)
                              +string(" :: ")+pagoda::to_string(line))
        {}
        explicit DataTypeException(const string &where,
                                   const string &what, const DataType &type) :   runtime_error(where + ": " + what
                                               +string(" :: ")+type.get_name())
        {}
        explicit DataTypeException(const string &what, const DataType &type)
            :   runtime_error(what
                              +string(" :: ")+type.get_name())
        {}
        explicit DataTypeException(const string &what, const DataType &type,
                                   const char *file, int line)
            :   runtime_error(what
                              +string(" :: ")+type.get_name()
                              +string(" :: ")+string(file)
                              +string(" :: ")+pagoda::to_string(line))
        {}
};


template <class T> DataType DataType::ctype()
{
    throw DataTypeException("invalid C type in translate_type");
}


#endif // DATATYPE_H_
