#ifndef VARIABLE_H_
#define VARIABLE_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "Attribute.H"
#include "DataType.H"
#include "Dimension.H"


class Variable
{
    public:
        Variable();
        Variable(int ncid, int varid, string file="");
        Variable(const Variable &copy);
        virtual ~Variable();

        Variable& operator = (const Variable &other);
        bool operator == (const Variable &other) const;

        string get_name() const;

        bool has_record() const;
        size_t num_dimensions() const;
        vector<Dimension> get_dimensions() const;

        size_t num_attributes() const;
        vector<Attribute> get_attributes() const;

        DataType get_type() const;

        friend ostream& operator << (ostream &os, const Variable &other);

    private:
        string name;
        vector<Dimension> dimensions;
        vector<Attribute> attributes;
        string file;
        DataType type;
};

#endif // VARIABLE_H_
