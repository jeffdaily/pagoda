#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <sstream>

#include "AbstractVariable.H"
#include "Array.H"
#include "Attribute.H"
#include "Dimension.H"
#include "Mask.H"
#include "StringComparator.H"
#include "Timing.H"
#include "Variable.H"

using std::ostringstream;


AbstractVariable::AbstractVariable()
    :   Variable()
{
    TIMING("AbstractVariable::AbstractVariable()");
}


AbstractVariable::~AbstractVariable()
{
    TIMING("AbstractVariable::~AbstractVariable()");
}


bool AbstractVariable::has_record() const
{
    TIMING("AbstractVariable::has_record()");
    return get_dims()[0]->is_unlimited();
}


int64_t AbstractVariable::num_dims() const
{
    TIMING("AbstractVariable::num_dims()");
    return get_dims().size();
}


vector<int64_t> AbstractVariable::get_shape() const
{
    TIMING("AbstractVariable::get_sizes(bool)");
    vector<int64_t> shape;
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::const_iterator it = dims.begin();
    vector<Dimension*>::const_iterator end = dims.end();

    for (; it!=end; ++it) {
        shape.push_back((*it)->get_size());
    }

    return shape;
}


int64_t AbstractVariable::num_atts() const
{
    TIMING("AbstractVariable::num_atts()");
    return get_atts().size();
}


string AbstractVariable::get_long_name() const
{
    TIMING("AbstractVariable::get_long_name()");
    string att_name("long_name");
    Attribute *att = get_att(att_name);

    if (att) {
        ostringstream val;
        val << att->get_values();
        return val.str();
    }
    return "";
}


Attribute* AbstractVariable::get_att(
        const string &name,
        bool ignore_case) const
{
    TIMING("AbstractVariable::get_att(string,bool)");
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::const_iterator it = atts.begin();
    vector<Attribute*>::const_iterator end = atts.end();
    StringComparator cmp;

    cmp.set_ignore_case(ignore_case);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name()); // set to current atts' name
        if (cmp(name)) {
            return *it;
        }
    }

    return NULL;
}


Attribute* AbstractVariable::get_att(
        const vector<string> &names,
        bool ignore_case) const
{
    TIMING("AbstractVariable::get_att(vector<string>,bool)");
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::const_iterator it = atts.begin();
    vector<Attribute*>::const_iterator end = atts.end();
    StringComparator cmp;

    cmp.set_ignore_case(ignore_case);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name()); // set to current atts' name
        if (cmp(names)) {
            return *it;
        }
    }

    return NULL;
}


Array* AbstractVariable::read()
{
    Array *dst = Array::create(get_type(), get_shape());
    return read(dst);
}


Array* AbstractVariable::read(int64_t record)
{
    vector<int64_t> shape;
    Array *dst;

    shape = get_shape();
    shape.erase(shape.begin());
    dst = Array::create(get_type(), shape);

    return read(record, dst);
}


ostream& AbstractVariable::print(ostream &os) const
{
    TIMING("AbstractVariable::print(ostream)");
    const string name(get_name());
    return os << "AbstractVariable(" << name << ")";
}
