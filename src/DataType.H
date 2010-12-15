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

        /**
         * Convert either a Global Arrays type or a netcdf type to a DataType.
         *
         * Luckily the GA types are integers within 1000 and 1016 and the
         * netcdf types are from 0 to 6.
         *
         * @param[in] type the integer to convert to a DataType
         */
        DataType(int type);

        /**
         * Copy constructor.
         *
         * @param[in] type the DataType to copy
         */
        DataType(const DataType &type);

        /**
         * Assignment to an intger.
         *
         * @param[in] type the integer to convert to a DataType
         * @return this DataType
         */
        DataType& operator = (int type);

        /**
         * Assignment to another DataType.
         *
         * @param[in] type the other DataType
         * @return this DataType
         */
        DataType& operator = (const DataType &type);

        /**
         * Equality comparison.
         *
         * @param[in] type the DataType to compare
         * @return true if the IDs are equal
         */
        bool operator == (const DataType &type) const;

        /**
         * Inequality comparison.
         *
         * @param[in] type the DataType to compare
         * @return true if the IDs are not equal
         */
        bool operator != (const DataType &type) const;

        /**
         * Inserts the name of the DataType into the given stream.
         *
         * @param[in,out] os the stream
         * @param[in] type the DataType to insert
         * @return the given stream
         */
        friend ostream& operator << (ostream &os, const DataType &type);

        /**
         * Convert DataType to Global Arrays type.
         *
         * @return the Global Arrays type
         */
        int to_ga() const;

        /**
         * Convert DataType to netcdf type.
         *
         * @return the netcdf type
         */
        nc_type to_nc() const;

        int get_id() const {
            return id;
        }

        /**
         * Returns the string represenation of this DataType.
         */
        string get_name() const {
            return name;
        }

        /**
         * Returns the C type size for this DataType.
         */
        int64_t get_bytes() const;

        /**
         * Returns the DataType instance for a given C type.
         */
        template <class T> static DataType ctype();

    protected:
        /**
         * Convert int to DataType.
         *
         * @param[in] type the integer to convert
         * @return the corresponding DataType
         */
        static DataType to_dt(int type);

        /**
         * Constructs DataType with a name.
         */
        DataType(const string &name);

        static int next_id; /**< id of next DataType constructed */
        int id; /**< id of this DataType */
        string name; /**< name of this DataType */
};


/**
 * An exception as a result of a DataType mismatch, conversion problem, etc.
 */
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
                                   const string &what, const DataType &type)
            :   runtime_error(where + ": " + what
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
