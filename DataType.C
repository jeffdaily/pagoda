#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <macdecls.h>
#if HAVE_PNETCDF_H
#   include <pnetcdf.h>
#endif
#if HAVE_NETCDF_H
#   include <netcdf.h>
#endif

#include "DataType.H"
#include "Timing.H"


DataType::DataType(nc_type type)
    :   value(DataType::to_dt(type))
{
    TIMING("DataType::DataType(nc_type)");
}


DataType::DataType(int type)
    :   value(DataType::to_dt(type))
{
    TIMING("DataType(int)");
}


DataType::DataType(const DataType &type)
    :   value(type.value)
{
    TIMING("DataType(DataType)");
}


DataType& DataType::operator = (nc_type type)
{
    TIMING("DataType::operator=(nc_type)");
    value = DataType::to_dt(type);
    return *this;
}


DataType& DataType::operator = (int type)
{
    TIMING("DataType::operator=(int)");
    value = DataType::to_dt(type);
    return *this;
}


DataType& DataType::operator = (const DataType &type)
{
    TIMING("DataType::operator=(DataType)");
    value = type.value;
    return *this;
}


DataType::operator nc_type() const
{
    TIMING("DataType::operator nc_type()");
    return DataType::to_nc(value);
}


DataType::operator int () const
{
    TIMING("DataType::operator int()");
    return DataType::to_ma(value);
}


nc_type DataType::as_nc() const
{
    TIMING("DataType::as_nc()");
    return DataType::to_nc(value);
}


int DataType::as_ma() const
{
    TIMING("DataType::as_ma()");
    return DataType::to_ma(value);
}


bool DataType::operator == (nc_type type) const
{
    TIMING("DataType::operator==(nc_type)");
    return value == DataType::to_dt(type);
}


bool DataType::operator == (int type) const
{
    TIMING("DataType::operator==(int)");
    return value == DataType::to_dt(type);
}


bool DataType::operator == (const DataType &type) const
{
    TIMING("DataType::operator==(DataType)");
    return value == type.value;
}


ostream& operator << (ostream &os, const DataType &type)
{
    TIMING("operator<<(ostream,DataType)");
    if (DataType::dt_char == type) {
        os << "char";
    } else if (DataType::dt_short == type) {
        os << "short";
    } else if (DataType::dt_int == type) {
        os << "int";
    } else if (DataType::dt_long == type) {
        os << "long";
    } else if (DataType::dt_longlong == type) {
        os << "long long";
    } else if (DataType::dt_float == type) {
        os << "float";
    } else if (DataType::dt_double == type) {
        os << "double";
    } else if (DataType::dt_longdouble == type) {
        os << "long double";
    } else if (DataType::dt_uchar == type) {
        os << "unsigned char";
    } else if (DataType::dt_ushort == type) {
        os << "unsigned short";
    } else if (DataType::dt_uint == type) {
        os << "unsigned int";
    } else if (DataType::dt_ulong == type) {
        os << "unsigned long";
    } else if (DataType::dt_ulonglong == type) {
        os << "unsigned long long";
    } else if (DataType::dt_string == type) {
        os << "string";
    } else {
        os << "UNKNOWN";
    }
    return os;
}


/**
 * Convert nc_type to dt_type.
 *
 * Although nc_type is typically a typedef for int and therefore
 * DataType::to_dt(int) could be used instead, the intention here is that this
 * function converts nc_type.
 */
DataType::dt_type DataType::to_dt(nc_type type)
{
    TIMING("DataType::to_dt(nc_type)");
    if (NC_NAT == type) { /* 'Not A Type' */
        throw DataTypeException("NC_NAT not supported");
    } else if (NC_BYTE == type) { /* signed 1 byte integer */
        return dt_char;
    } else if (NC_CHAR == type) { /* ISO/ASCII character */
        return dt_uchar;
    } else if (NC_SHORT == type) { /* signed 2 byte integer */
#if 2 == SIZEOF_SHORT
        return dt_short;
#elif 2 == SIZEOF_INT
        return dt_int;
#elif 2 == SIZEOF_LONG
        return dt_long;
#elif 2 == SIZEOF_LONG_LONG
        return dt_longlong;
#else
        throw DataTypeException("no corresponding C type for NC_SHORT");
#endif
    } else if (NC_INT == type) { /* signed 4 byte integer */
#if 4 == SIZEOF_SHORT
        return dt_short;
#elif 4 == SIZEOF_INT
        return dt_int;
#elif 4 == SIZEOF_LONG
        return dt_long;
#elif 4 == SIZEOF_LONG_LONG
        return dt_longlong;
#else
        throw DataTypeException("no corresponding C type for NC_INT");
#endif
    } else if (NC_FLOAT == type) { /* single precision floating point number */
#if 4 == SIZEOF_FLOAT
        return dt_float;
#elif 4 == SIZEOF_DOUBLE
        return dt_double;
#elif 4 == SIZEOF_LONG_DOUBLE
        return dt_longdouble;
#else
        throw DataTypeException("no corresponding C type for NC_FLOAT");
#endif
    } else if (NC_DOUBLE == type) { /* double precision floating point number */
#if 8 == SIZEOF_FLOAT
        return dt_float;
#elif 8 == SIZEOF_DOUBLE
        return dt_double;
#elif 8 == SIZEOF_LONG_DOUBLE
        return dt_longdouble;
#else
        throw DataTypeException("no corresponding C type for NC_DOUBLE");
#endif
        return dt_double;
#ifdef HAVE_NETCDF4
    } else if (NC_UBYTE == type) { /* unsigned 1 byte integer */
        return dt_uchar;
    } else if (NC_USHORT == type) { /* unsigned 2 byte integer */
        return dt_ushort;
    } else if (NC_UINT == type) { /* unsigned 4 byte integer */
        return dt_uint;
    } else if (NC_INT64 == type) { /* signed 8 byte integer */
#if 8 == SIZEOF_INT
        return dt_int;
#elif 8 == SIZEOF_LONG
        return dt_long;
#elif 8 == SIZEOF_LONG_LONG
        return dt_longlong;
#else
        throw DataTypeException("no corresponding C type for NC_INT64");
#endif
    } else if (NC_UINT64 == type) { /* unsigned 8 byte integer */
    } else if (NC_STRING == type) { /* string */
#endif /* HAVE_NETCDF4 */
    }
    throw DataTypeException("could not determine dt_type from nc_type");
}


/**
 * Convert the MA type to a dt_type.
 * 
 * @see DataType::to_dt(nc_type)
 */
DataType::dt_type DataType::to_dt(int type)
{
    TIMING("DataType::to_dt(int)");
    if (MT_C_CHAR == type) {
        return dt_char;
    } else if (MT_C_INT == type) {
        return dt_int;
    } else if (MT_C_LONGINT == type) {
        return dt_long;
    } else if (MT_C_LONGLONG == type) {
        return dt_longlong;
    } else if (MT_C_FLOAT == type) {
        return dt_float;
    } else if (MT_C_DBL == type) {
        return dt_double;
    } else if (MT_C_LDBL == type) {
        return dt_longdouble;
    }
    throw DataTypeException("could not determine dt_type from int (MA)");
}


nc_type DataType::to_nc(dt_type type)
{
    TIMING("DataType::to_nc(dt_type)");
    if (dt_char == type) {
        return NC_BYTE;
    } else if (dt_short == type) {
#if 2 == SIZEOF_SHORT
        return NC_SHORT;
#elif 4 == SIZEOF_SHORT
        return NC_INT;
#elif 8 == SIZEOF_INT && HAVE_NETCDF4
        return NC_INT64;
#else
        throw DataTypeException("could not determine nc_type from dt_short");
#endif
    } else if (dt_int == type) {
#if 2 == SIZEOF_INT
        return NC_SHORT;
#elif 4 == SIZEOF_INT
        return NC_INT;
#elif 8 == SIZEOF_INT && HAVE_NETCDF4
        return NC_INT64;
#else
        throw DataTypeException("could not determine nc_type from dt_int");
#endif
    } else if (dt_long == type) {
#if 2 == SIZEOF_LONG
        return NC_SHORT;
#elif 4 == SIZEOF_LONG
        return NC_INT;
#elif 8 == SIZEOF_LONG && HAVE_NETCDF4
        return NC_INT64;
#else
        throw DataTypeException("could not determine nc_type from dt_long");
#endif
    } else if (dt_longlong == type) {
#if 2 == SIZEOF_LONG_LONG
        return NC_SHORT;
#elif 4 == SIZEOF_LONG_LONG
        return NC_INT;
#elif 8 == SIZEOF_LONG_LONG && HAVE_NETCDF4
        return NC_INT64;
#else
        throw DataTypeException("could not determine nc_type from dt_longlong");
#endif
    } else if (dt_float == type) {
#if 4 == SIZEOF_FLOAT
        return NC_FLOAT;
#elif 8 == SIZEOF_FLOAT
        return NC_DOUBLE;
#else
        throw DataTypeException("could not determine nc_type from dt_float");
#endif
    } else if (dt_double == type) {
#if 4 == SIZEOF_DOUBLE
        return NC_FLOAT;
#elif 8 == SIZEOF_DOUBLE
        return NC_DOUBLE;
#else
        throw DataTypeException("could not determine nc_type from dt_double");
#endif
    } else if (dt_longdouble == type) {
#if 4 == SIZEOF_LONG_DOUBLE
        return NC_FLOAT;
#elif 8 == SIZEOF_LONG_DOUBLE
        return NC_DOUBLE;
#else
        throw DataTypeException("could not determine nc_type from dt_longdouble");
#endif
    } else if (dt_uchar == type) {
        return NC_CHAR;
    } else if (dt_ushort == type) {
        throw DataTypeException("could not determine nc_type from dt_ushort");
    } else if (dt_uint == type) {
        throw DataTypeException("could not determine nc_type from dt_uint");
    } else if (dt_ulong == type) {
        throw DataTypeException("could not determine nc_type from dt_ulong");
    } else if (dt_ulonglong == type) {
        throw DataTypeException("could not determine nc_type from dt_ulonglong");
    } else if (dt_string == type) {
        throw DataTypeException("could not determine nc_type from dt_string");
    }
    throw DataTypeException("could not determine nc_type from dt_type");
}


int DataType::to_ma(dt_type type)
{
    TIMING("DataType::to_ma(dt_type)");
    if (type == dt_char) {
        return MT_C_CHAR;
    } else if (type == dt_short) {
        throw DataTypeException("MA does not support dt_short");
    } else if (type == dt_int) {
        return MT_C_INT;
    } else if (type == dt_long) {
        return MT_C_LONGINT;
    } else if (type == dt_longlong) {
        return MT_C_LONGLONG;
    } else if (type == dt_float) {
        return MT_C_FLOAT;
    } else if (type == dt_double) {
        return MT_C_DBL;
    } else if (type == dt_longdouble) {
        return MT_C_LDBL;
    } else if (type == dt_uchar) {
        throw DataTypeException("MA does not support dt_uchar");
    } else if (type == dt_ushort) {
        throw DataTypeException("MA does not support dt_ushort");
    } else if (type == dt_uint) {
        throw DataTypeException("MA does not support dt_uint");
    } else if (type == dt_ulong) {
        throw DataTypeException("MA does not support dt_ulong");
    } else if (type == dt_ulonglong) {
        throw DataTypeException("MA does not support dt_ulonglong");
    } else if (type == dt_string) {
        throw DataTypeException("MA does not support dt_string");
    }
    throw DataTypeException("could not determine MA type from dt_type");
}
