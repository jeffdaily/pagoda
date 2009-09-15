#ifndef VARIABLE_H_
#define VARIABLE_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "DataType.H"

class Attribute;
class Dimension;


class Variable
{
    public:
        virtual ~Variable();

        virtual string get_name() const = 0;

        virtual vector<Dimension*> get_dims() const = 0;
        virtual bool has_record() const = 0;
        virtual size_t num_dims() const = 0;
        virtual int64_t get_size(bool use_masks=false) const = 0;
        virtual vector<int64_t> get_sizes(bool use_masks=false) const = 0;
        virtual bool needs_subset() const = 0;

        virtual vector<Attribute*> get_atts() const = 0;
        virtual size_t num_atts() const = 0;
        virtual string get_long_name() const = 0;
        virtual Attribute* find_att(const string &name,
                bool ignore_case=false) const = 0;
        virtual Attribute* find_att(const vector<string> &names,
                bool ignore_case=false) const = 0;

        virtual DataType get_type() const = 0;

        virtual int get_handle() = 0;
        virtual void release_handle() = 0;
        virtual void read() = 0;
        virtual void set_record_index(size_t index) = 0;
        virtual size_t get_record_index() const = 0;

        virtual void reindex() = 0;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Variable *other);

    protected:
        Variable();
};

#endif // VARIABLE_H_
