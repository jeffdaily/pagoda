#ifndef ABSTRACTVARIABLE_H_
#define ABSTRACTVARIABLE_H_

#include <iostream>
#include <string>
#include <vector>

#include "DataType.H"
#include "Variable.H"

using std::ostream;
using std::string;
using std::vector;

class Attribute;


class AbstractVariable : public Variable
{
    public:
        virtual ~AbstractVariable();

        virtual bool has_record() const;
        virtual size_t num_dims() const;
        virtual int64_t get_size(bool use_masks=false) const;
        virtual vector<int64_t> get_sizes(bool use_masks=false) const;
        virtual bool needs_subset() const;

        virtual size_t num_atts() const;
        virtual string get_long_name() const;
        virtual Attribute* find_att(const string &name,
                bool ignore_case=false) const;
        virtual Attribute* find_att(const vector<string> &names,
                bool ignore_case=false) const;

        virtual int get_handle();
        virtual void release_handle();
        virtual void set_record_index(size_t index);
        virtual size_t get_record_index() const;

        virtual void reindex();

        virtual ostream& print(ostream &os) const;

    protected:
        AbstractVariable();

        int *handle;
        size_t record_index;
};

#endif // ABSTRACTVARIABLE_H_
