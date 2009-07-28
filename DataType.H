#ifndef DATATYPE_H_
#define DATATYPE_H_

#include <iostream>
    using std::ostream;

#include <macdecls.h>
#include <pnetcdf.h>


class DataType
{
    private:
        static int counter;

        DataType();
        int value;

    public:
        bool operator == (const DataType &other) const;
        DataType& operator = (const nc_type &type);
        DataType& operator = (int value);
        operator int () const;
        operator nc_type () const;
        int as_mt() const;
        void from_mt(int mt);
        friend ostream& operator << (ostream &os, const DataType &other);

        static const DataType BYTE;
        static const DataType CHAR;
        static const DataType SHORT;
        static const DataType INT;
        //static const DataType LONG;
        static const DataType FLOAT;
        static const DataType DOUBLE;
};

#endif // DATATYPE_H_
