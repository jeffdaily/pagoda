#include "DataType.H"

int DataType::counter(0);
const DataType DataType::BYTE;
const DataType DataType::CHAR;
const DataType DataType::SHORT;
const DataType DataType::INT;
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


ostream& operator << (ostream &os, const DataType &other)
{
    if (other == DataType::BYTE) os << "byte";
    if (other == DataType::CHAR) os << "char";
    if (other == DataType::SHORT) os << "short";
    if (other == DataType::INT) os << "int";
    if (other == DataType::FLOAT) os << "float";
    if (other == DataType::DOUBLE) os << "double";
    return os;
}

