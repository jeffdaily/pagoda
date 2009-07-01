#ifndef NETCDFATTRIBUTE_H_
#define NETCDFATTRIBUTE_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;

#include "Attribute.H"
#include "DataType.H"

class Values;


class NetcdfAttribute : public Attribute
{
    public:
        NetcdfAttribute(int ncid, int attid, int varid);
        NetcdfAttribute(const NetcdfAttribute &copy);
        virtual ~NetcdfAttribute();

        virtual string get_name() const;
        virtual DataType get_type() const;
        virtual size_t get_count() const;
        virtual bool is_global() const;
        virtual Values* get_values() const;

        virtual ostream& print(ostream &os) const;

    private:
        int ncid;
        int id;
        int varid;
        string name;
        DataType type;
        bool global;
        Values *values;
};

#endif // NETCDFATTRIBUTE_H_
