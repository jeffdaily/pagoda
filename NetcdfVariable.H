#ifndef NETCDFVARIABLE_H_
#define NETCDFVARIABLE_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "DataType.H"
#include "Variable.H"

class Attribute;
class Dimension;
class NetcdfAttribute;
class NetcdfDimension;


class NetcdfVariable : public Variable
{
    public:
        NetcdfVariable(int ncid, int varid);
        NetcdfVariable(const NetcdfVariable &copy);
        virtual ~NetcdfVariable();

        NetcdfVariable& operator = (const NetcdfVariable &other);

        virtual string get_name() const;

        virtual bool has_record() const;
        virtual size_t num_dims() const;
        virtual vector<Dimension*> get_dims() const;

        virtual size_t num_atts() const;
        virtual vector<Attribute*> get_atts() const;

        DataType get_type() const;

        virtual ostream& print(ostream &os) const;

    private:
        int ncid;
        int id;
        string name;
        vector<NetcdfDimension*> dims;
        vector<NetcdfAttribute*> atts;
        DataType type;
};

#endif // NETCDFVARIABLE_H_
