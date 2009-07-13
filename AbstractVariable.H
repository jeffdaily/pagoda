#ifndef ABSTRACTVARIABLE_H_
#define ABSTRACTVARIABLE_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "DataType.H"
#include "Variable.H"

class Attribute;


class AbstractVariable : public Variable
{
    public:
        virtual ~AbstractVariable();

        virtual int64_t get_size() const;
        virtual int64_t* get_sizes() const;
        virtual size_t num_masks() const;

        virtual string get_long_name() const;
        virtual Attribute* find_att(const string &name,
                bool ignore_case=true) const;
        virtual Attribute* find_att(const vector<string> &names,
                bool ignore_case=true) const;

        virtual int get_handle();
        virtual void release_handle();
        virtual void set_record_index(size_t index);
        virtual size_t get_record_index() const;

        virtual ostream& print(ostream &os) const;

    protected:
        AbstractVariable();

        int handle;
        size_t record_index;
};

#endif // ABSTRACTVARIABLE_H_
