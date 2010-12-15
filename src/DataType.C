#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#if HAVE_GA
#   include <ga.h>
#   include <macommon.h>
#endif

#if HAVE_PNETCDF
#   include <pnetcdf.h>
#elif HAVE_NETCDF
#   include <netcdf.h>
#endif

#include "DataType.H"
#include "Timing.H"


const DataType DataType::CHAR("char");
const DataType DataType::SHORT("short");
const DataType DataType::INT("int");
const DataType DataType::LONG("long");
const DataType DataType::LONGLONG("long long");
const DataType DataType::FLOAT("float");
const DataType DataType::DOUBLE("double");
const DataType DataType::LONGDOUBLE("long double");
const DataType DataType::UCHAR("unsigned char");
const DataType DataType::USHORT("unsigned short");
const DataType DataType::UINT("unsigned int");
const DataType DataType::ULONG("unsigned long");
const DataType DataType::ULONGLONG("unsigned long long");
const DataType DataType::SCHAR("signed char");
const DataType DataType::STRING("string");
int DataType::next_id(1);


DataType::DataType(const string &name)
    :   id(next_id++)
    ,   name(name)
{
}


DataType::DataType(int type)
    :   id(-1)
    ,   name("")
{
    DataType dt = DataType::to_dt(type);
    id = dt.id;
    name = dt.name;
}


DataType::DataType(const DataType &type)
    :   id(type.id)
    ,   name(type.name)
{
}


DataType& DataType::operator = (int type)
{
    *this = DataType::to_dt(type);
    return *this;
}


DataType& DataType::operator = (const DataType &type)
{
    id = type.id;
    name = type.name;
    return *this;
}


bool DataType::operator == (const DataType &type) const
{
    return id == type.id;
}


bool DataType::operator != (const DataType &type) const
{
    return id != type.id;
}


ostream& operator << (ostream &os, const DataType &type)
{
    return os << type.name;
}


int DataType::to_ga() const
{
    TIMING("DataType::to_ga()");

    if (false) {
        /*
        } else if (operator==(DataType::CHAR)) {
            return C_CHAR;
        */
    }
    else if (operator==(DataType::SHORT)) {
        throw DataTypeException("GA does not support C short");
    }
    else if (operator==(DataType::INT)) {
        return C_INT;
    }
    else if (operator==(DataType::LONG)) {
        return C_LONG;
    }
    else if (operator==(DataType::LONGLONG)) {
        return C_LONGLONG;
    }
    else if (operator==(DataType::FLOAT)) {
        return C_FLOAT;
    }
    else if (operator==(DataType::DOUBLE)) {
        return C_DBL;
    }
    else if (operator==(DataType::LONGDOUBLE)) {
#ifdef C_LDBL
        return C_LDBL;
#else
        throw DataTypeException("GA does not support C long double");
#endif
    }
    else if (operator==(DataType::UCHAR)) {
        throw DataTypeException("GA does not support C unsigned char");
    }
    else if (operator==(DataType::USHORT)) {
        throw DataTypeException("GA does not support C unsigned short");
    }
    else if (operator==(DataType::UINT)) {
        throw DataTypeException("GA does not support C unsigned int");
    }
    else if (operator==(DataType::ULONG)) {
        throw DataTypeException("GA does not support C unsigned long");
    }
    else if (operator==(DataType::ULONGLONG)) {
        throw DataTypeException("GA does not support C unsigned long long");
    }
    else if (operator==(DataType::STRING)) {
        throw DataTypeException("GA does not support C char*");
    }

    throw DataTypeException("could not determine GA type from DataType");
}


nc_type DataType::to_nc() const
{
    TIMING("DataType::to_nc()");

    if (operator==(DataType::CHAR)) {
        return NC_CHAR;
    }
    else if (operator==(DataType::SCHAR)) {
        return NC_BYTE;
    }
    else if (operator==(DataType::SHORT)) {
#if   2 == SIZEOF_SHORT
        return NC_SHORT;
#elif 4 == SIZEOF_SHORT
        return NC_INT;
#elif 8 == SIZEOF_INT && HAVE_NETCDF4
        return NC_INT64;
#else
        throw DataTypeException("could not determine nc_type from SHORT");
#endif
    }
    else if (operator==(DataType::INT)) {
#if   2 == SIZEOF_INT
        return NC_SHORT;
#elif 4 == SIZEOF_INT
        return NC_INT;
#elif 8 == SIZEOF_INT && HAVE_NETCDF4
        return NC_INT64;
#else
        throw DataTypeException("could not determine nc_type from INT");
#endif
    }
    else if (operator==(DataType::LONG)) {
#if   2 == SIZEOF_LONG
        return NC_SHORT;
#elif 4 == SIZEOF_LONG
        return NC_INT;
#elif 8 == SIZEOF_LONG && HAVE_NETCDF4
        return NC_INT64;
#else
        throw DataTypeException("could not determine nc_type from LONG");
#endif
    }
    else if (operator==(DataType::LONGLONG)) {
#if   2 == SIZEOF_LONG_LONG
        return NC_SHORT;
#elif 4 == SIZEOF_LONG_LONG
        return NC_INT;
#elif 8 == SIZEOF_LONG_LONG && HAVE_NETCDF4
        return NC_INT64;
#else
        throw DataTypeException("could not determine nc_type from LONGLONG");
#endif
    }
    else if (operator==(DataType::FLOAT)) {
#if   4 == SIZEOF_FLOAT
        return NC_FLOAT;
#elif 8 == SIZEOF_FLOAT
        return NC_DOUBLE;
#else
        throw DataTypeException("could not determine nc_type from FLOAT");
#endif
    }
    else if (operator==(DataType::DOUBLE)) {
#if   4 == SIZEOF_DOUBLE
        return NC_FLOAT;
#elif 8 == SIZEOF_DOUBLE
        return NC_DOUBLE;
#else
        throw DataTypeException("could not determine nc_type from DOUBLE");
#endif
    }
    else if (operator==(DataType::LONGDOUBLE)) {
#if   4 == SIZEOF_LONG_DOUBLE
        return NC_FLOAT;
#elif 8 == SIZEOF_LONG_DOUBLE
        return NC_DOUBLE;
#else
        throw DataTypeException("could not determine nc_type from LONGDOUBLE");
#endif
    }
    else if (operator==(DataType::UCHAR)) {
#if HAVE_NETCDF4
        return NC_UBYTE;
#else
        throw DataTypeException("could not determine nc_type from UCHAR");
#endif
    }
    else if (operator==(DataType::USHORT)) {
#if   HAVE_NETCDF4 && 2 == SIZEOF_UNSIGNED_SHORT
        return NC_USHORT;
#elif HAVE_NETCDF4 && 4 == SIZEOF_UNSIGNED_SHORT
        return NC_UINT;
#elif HAVE_NETCDF4 && 8 == SIZEOF_UNSIGNED_SHORT
        return NC_UINT64;
#else
        throw DataTypeException("could not determine nc_type from USHORT");
#endif
    }
    else if (operator==(DataType::UINT)) {
#if   HAVE_NETCDF4 && 2 == SIZEOF_UNSIGNED_INT
        return NC_USHORT;
#elif HAVE_NETCDF4 && 4 == SIZEOF_UNSIGNED_INT
        return NC_UINT;
#elif HAVE_NETCDF4 && 8 == SIZEOF_UNSIGNED_INT
        return NC_UINT64;
#else
        throw DataTypeException("could not determine nc_type from UINT");
#endif
    }
    else if (operator==(DataType::ULONG)) {
#if   HAVE_NETCDF4 && 2 == SIZEOF_UNSIGNED_LONG
        return NC_USHORT;
#elif HAVE_NETCDF4 && 4 == SIZEOF_UNSIGNED_LONG
        return NC_UINT;
#elif HAVE_NETCDF4 && 8 == SIZEOF_UNSIGNED_LONG
        return NC_UINT64;
#else
        throw DataTypeException("could not determine nc_type from ULONG");
#endif
    }
    else if (operator==(DataType::ULONGLONG)) {
#if   HAVE_NETCDF4 && 2 == SIZEOF_UNSIGNED_LONG_LONG
        return NC_USHORT;
#elif HAVE_NETCDF4 && 4 == SIZEOF_UNSIGNED_LONG_LONG
        return NC_UINT;
#elif HAVE_NETCDF4 && 8 == SIZEOF_UNSIGNED_LONG_LONG
        return NC_UINT64;
#else
        throw DataTypeException("could not determine nc_type from ULONGLONG");
#endif
    }
    else if (operator==(DataType::STRING)) {
#if   HAVE_NETCDF4
        return NC_STRING;
#else
        throw DataTypeException("could not determine nc_type from STRING");
#endif
    }

    throw DataTypeException("could not determine nc_type");
}


int64_t DataType::get_bytes() const
{
    if (operator==(DataType::CHAR)) {
        return sizeof(char);
    }
    else if (operator==(DataType::SHORT)) {
        return sizeof(short);
    }
    else if (operator==(DataType::INT)) {
        return sizeof(int);
    }
    else if (operator==(DataType::LONG)) {
        return sizeof(long);
    }
    else if (operator==(DataType::LONGLONG)) {
        return sizeof(long long);
    }
    else if (operator==(DataType::FLOAT)) {
        return sizeof(float);
    }
    else if (operator==(DataType::DOUBLE)) {
        return sizeof(double);
    }
    else if (operator==(DataType::LONGDOUBLE)) {
        return sizeof(long double);
    }
    else if (operator==(DataType::UCHAR)) {
        return sizeof(unsigned char);
    }
    else if (operator==(DataType::USHORT)) {
        return sizeof(unsigned short);
    }
    else if (operator==(DataType::UINT)) {
        return sizeof(unsigned int);
    }
    else if (operator==(DataType::ULONG)) {
        return sizeof(unsigned long);
    }
    else if (operator==(DataType::ULONGLONG)) {
        return sizeof(unsigned long long);
    }
    else if (operator==(DataType::SCHAR)) {
        return sizeof(signed char);
    }
    else if (operator==(DataType::STRING)) {
        return sizeof(char);
    }
    else {
        throw DataTypeException("could not determine DataType for C size");
    }
}


DataType DataType::to_dt(int type)
{
    TIMING("DataType::to_dt(int)");

    if (NC_NAT == type) {        // Not A Type
        throw DataTypeException("NC_NAT not supported");
    }
    else if (NC_BYTE == type) {   // signed 1 byte integer
        return DataType::SCHAR;
    }
    else if (NC_CHAR == type) {   // ISO/ASCII character
        return DataType::CHAR;
    }
    else if (NC_SHORT == type) {   // signed 2 byte integer
#if   2 == SIZEOF_SHORT
        return DataType::SHORT;
#elif 2 == SIZEOF_INT
        return DataType::INT;
#elif 2 == SIZEOF_LONG
        return DataType::LONG;
#elif 2 == SIZEOF_LONG_LONG
        return DataType::LONGLONG;
#else
        throw DataTypeException("no corresponding C type for NC_SHORT");
#endif
    }
    else if (NC_INT == type) {   // signed 4 byte integer
#if   4 == SIZEOF_SHORT
        return DataType::SHORT;
#elif 4 == SIZEOF_INT
        return DataType::INT;
#elif 4 == SIZEOF_LONG
        return DataType::LONG;
#elif 4 == SIZEOF_LONG_LONG
        return DataType::LONLONG;
#else
        throw DataTypeException("no corresponding C type for NC_INT");
#endif
    }
    else if (NC_FLOAT == type) {   // single precision floating point number
#if   4 == SIZEOF_FLOAT
        return DataType::FLOAT;
#elif 4 == SIZEOF_DOUBLE
        return DataType::DOUBLE;
#elif 4 == SIZEOF_LONG_DOUBLE
        return DOUBLE::LONGDOUBLE;
#else
        throw DataTypeException("no corresponding C type for NC_FLOAT");
#endif
    }
    else if (NC_DOUBLE == type) {   // double precision floating point number
#if   8 == SIZEOF_FLOAT
        return DataType::FLOAT;
#elif 8 == SIZEOF_DOUBLE
        return DataType::DOUBLE;
#elif 8 == SIZEOF_LONG_DOUBLE
        return DOUBLE::LONGDOUBLE;
#else
        throw DataTypeException("no corresponding C type for NC_DOUBLE");
#endif
#ifdef HAVE_NETCDF4
    }
    else if (NC_UBYTE == type) {   // unsigned 1 byte integer
        return DataType::UCHAR;
    }
    else if (NC_USHORT == type) {   // unsigned 2 byte integer
#   if   2 == SIZEOF_UNSIGNED_SHORT
        return DataType::USHORT;
#   elif 2 == SIZEOF_UNSIGNED_INT
        return DataType::UINT;
#   elif 2 == SIZEOF_UNSIGNED_LONG
        return DataType::ULONG;
#   elif 2 == SIZEOF_UNSIGNED_LONG_LONG
        return DataType::ULONGLONG;
#   else
        throw DataTypeException("no corresponding C type for NC_USHORT");
#   endif
    }
    else if (NC_UINT == type) {   // unsigned 4 byte integer
#   if   4 == SIZEOF_UNSIGNED_SHORT
        return DataType::USHORT;
#   elif 4 == SIZEOF_UNSIGNED_INT
        return DataType::UINT;
#   elif 4 == SIZEOF_UNSIGNED_LONG
        return DataType::ULONG;
#   elif 4 == SIZEOF_UNSIGNED_LONG_LONG
        return DataType::ULONGLONG;
#   else
        throw DataTypeException("no corresponding C type for NC_UINT");
#   endif
    }
    else if (NC_INT64 == type) {   // signed 8 byte integer
#   if   8 == SIZEOF_INT
        return DataType::INT;
#   elif 8 == SIZEOF_LONG
        return DataType::LONG;
#   elif 8 == SIZEOF_LONG_LONG
        return DataType::LONGLONG;
#   else
        throw DataTypeException("no corresponding C type for NC_INT64");
#   endif
    }
    else if (NC_UINT64 == type) {   /* unsigned 8 byte integer */
#   if   8 == SIZEOF_UNSIGNED_INT
        return DataType::UINT;
#   elif 8 == SIZEOF_UNSIGNED_LONG
        return DataType::ULONG;
#   elif 8 == SIZEOF_UNSIGNED_LONG_LONG
        return DataType::ULONGLONG;
#   else
        throw DataTypeException("no corresponding C type for NC_UINT64");
#   endif
    }
    else if (NC_STRING == type) {   /* string */
        throw DataTypeException("no corresponding C type for NC_STRING");
#endif /* HAVE_NETCDF4 */
        /*
        } else if (C_CHAR == type) {
            return DataType::CHAR;
        */
    }
    else if (C_INT == type) {
        return DataType::INT;
    }
    else if (C_LONG == type) {
        return DataType::LONG;
    }
    else if (C_LONGLONG == type) {
        return DataType::LONGLONG;
    }
    else if (C_FLOAT == type) {
        return DataType::FLOAT;
    }
    else if (C_DBL == type) {
        return DataType::DOUBLE;
#ifdef C_LDBL
    }
    else if (C_LDBL == type) {
        return DataType::LONGDOUBLE;
#endif
    }
    throw DataTypeException("could not determine DataType from int");
}


template <> DataType DataType::ctype<char>()
{
    return DataType::CHAR;
}
template <> DataType DataType::ctype<short>()
{
    return DataType::SHORT;
}
template <> DataType DataType::ctype<int>()
{
    return DataType::INT;
}
template <> DataType DataType::ctype<long>()
{
    return DataType::LONG;
}
template <> DataType DataType::ctype<long long>()
{
    return DataType::LONGLONG;
}
template <> DataType DataType::ctype<float>()
{
    return DataType::FLOAT;
}
template <> DataType DataType::ctype<double>()
{
    return DataType::DOUBLE;
}
template <> DataType DataType::ctype<long double>()
{
    return DataType::LONGDOUBLE;
}
template <> DataType DataType::ctype<unsigned char>()
{
    return DataType::UCHAR;
}
template <> DataType DataType::ctype<unsigned short>()
{
    return DataType::USHORT;
}
template <> DataType DataType::ctype<unsigned int>()
{
    return DataType::UINT;
}
template <> DataType DataType::ctype<unsigned long>()
{
    return DataType::ULONG;
}
template <> DataType DataType::ctype<unsigned long long>()
{
    return DataType::ULONGLONG;
}
template <> DataType DataType::ctype<signed char>()
{
    return DataType::SCHAR;
}
template <> DataType DataType::ctype<std::string>()
{
    return DataType::STRING;
}
