#ifndef NETCDFVARIABLE_H_
#define NETCDFVARIABLE_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "AbstractVariable.H"
#include "DataType.H"

class Attribute;
class Dimension;
class NetcdfAttribute;
class NetcdfDimension;


class NetcdfVariable : public AbstractVariable
{
    public:
        NetcdfVariable(int ncid, int varid);
        virtual ~NetcdfVariable();

        virtual string get_name() const;

        virtual bool has_record() const;
        virtual size_t num_dims() const;
        virtual vector<Dimension*> get_dims() const;

        virtual size_t num_atts() const;
        virtual vector<Attribute*> get_atts() const;

        DataType get_type() const;

        virtual void read();

        virtual ostream& print(ostream &os) const;

        int get_ncid() const { return ncid; }
        int get_id() const { return id; }

    protected:
        int ncid;
        int id;
        string name;
        vector<NetcdfDimension*> dims;
        vector<NetcdfAttribute*> atts;
        DataType type;
};

#endif // NETCDFVARIABLE_H_
