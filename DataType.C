#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "DataType.H"

int DataType::counter(0);
const DataType DataType::BYTE;
const DataType DataType::CHAR;
const DataType DataType::SHORT;
const DataType DataType::INT;
//const DataType DataType::LONG;
const DataType DataType::FLOAT;
const DataType DataType::DOUBLE;


DataType::DataType()
    :   value(DataType::counter++)
{
}


DataType::operator int () const
{
    return value;
}


DataType::operator nc_type() const
{
    if (*this == DataType::BYTE) return NC_BYTE;
    if (*this == DataType::CHAR) return NC_CHAR;
    if (*this == DataType::SHORT) return NC_SHORT;
    if (*this == DataType::INT) return NC_INT;
    if (*this == DataType::FLOAT) return NC_FLOAT;
    if (*this == DataType::DOUBLE) return NC_DOUBLE;
    return NC_BYTE;
}


int DataType::as_mt() const
{
    if (*this == DataType::CHAR) return MT_CHAR;
    else if (*this == DataType::INT) return MT_INT;
    else if (*this == DataType::FLOAT) return MT_REAL;
    else if (*this == DataType::DOUBLE) return MT_DBL;
    else return MT_CHAR;
}


void DataType::from_mt(int mt)
{
    switch (mt) {
        case MT_CHAR: value = DataType::CHAR.value; break;
        case MT_INT: value = DataType::INT.value; break;
        case MT_REAL: value = DataType::FLOAT.value; break;
        case MT_DBL: value = DataType::DOUBLE.value; break;
    }
}


bool DataType::operator == (const DataType &other) const
{
    return value == other.value;
}


DataType& DataType::operator = (const nc_type &type)
{
    switch (type) {
        case NC_BYTE: value = DataType::BYTE; break;
        case NC_CHAR: value = DataType::CHAR; break;
        case NC_SHORT: value = DataType::SHORT; break;
        case NC_INT: value = DataType::INT; break;
        case NC_FLOAT: value = DataType::FLOAT; break;
        case NC_DOUBLE: value = DataType::DOUBLE; break;
    }

    return *this;
}


DataType& DataType::operator = (int value)
{
    switch (value) {
        case 0: value = DataType::BYTE; break;
        case 1: value = DataType::CHAR; break;
        case 2: value = DataType::SHORT; break;
        case 3: value = DataType::INT; break;
        case 4: value = DataType::FLOAT; break;
        case 5: value = DataType::DOUBLE; break;
        case MT_CHAR: value = DataType::CHAR; break;
        case MT_INT:  value = DataType::INT; break;
        case MT_REAL: value = DataType::FLOAT; break;
        case MT_DBL:  value = DataType::DOUBLE; break;
    }

    return *this;
}


ostream& operator << (ostream &os, const DataType &other)
{
    if (other == DataType::BYTE) os << "byte";
    else if (other == DataType::CHAR) os << "char";
    else if (other == DataType::SHORT) os << "short";
    else if (other == DataType::INT) os << "int";
    else if (other == DataType::FLOAT) os << "float";
    else if (other == DataType::DOUBLE) os << "double";
    else os << "UNKNOWN (" << other.value << ")";
    return os;
}

