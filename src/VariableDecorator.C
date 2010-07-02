#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <vector>

#include "Timing.H"
#include "VariableDecorator.H"

using std::vector;


VariableDecorator::VariableDecorator(Variable *var)
    :   Variable()
    ,   var(var)
{
    TIMING("VariableDecorator::VariableDecorator(Variable*)");
}


VariableDecorator::~VariableDecorator()
{
    TIMING("VariableDecorator::~VariableDecorator()");
    delete var;
    var = NULL;
}


string VariableDecorator::get_name() const
{
    TIMING("VariableDecorator::get_name()");
    return var->get_name();
}


vector<Dimension*> VariableDecorator::get_dims() const
{
    TIMING("VariableDecorator::get_dims()");
    return var->get_dims();
}


bool VariableDecorator::has_record() const
{
    TIMING("VariableDecorator::has_record()");
    return var->has_record();
}


int64_t VariableDecorator::num_dims() const
{
    TIMING("VariableDecorator::num_dims()");
    return var->num_dims();
}


vector<int64_t> VariableDecorator::get_shape() const
{
    TIMING("VariableDecorator::get_sizes()");
    return var->get_shape();
}


vector<Attribute*> VariableDecorator::get_atts() const
{
    TIMING("VariableDecorator::get_atts()");
    return var->get_atts();
}


int64_t VariableDecorator::num_atts() const
{
    TIMING("VariableDecorator::num_atts()");
    return var->num_atts();
}


string VariableDecorator::get_long_name() const
{
    TIMING("VariableDecorator::get_long_name()");
    return var->get_long_name();
}


Attribute* VariableDecorator::get_att(
        const string &name,
        bool ignore_case) const
{
    TIMING("VariableDecorator::get_att(string,bool)");
    return var->get_att(name, ignore_case);
}


Attribute* VariableDecorator::get_att(
        const vector<string> &names,
        bool ignore_case) const
{
    TIMING("VariableDecorator::get_att(vector<string>,bool)");
    return var->get_att(names, ignore_case);
}


DataType VariableDecorator::get_type() const
{
    TIMING("VariableDecorator::get_type()");
    return var->get_type();
}


Array* VariableDecorator::read()
{
    TIMING("VariableDecorator::read()");
    return var->read();
}


Array* VariableDecorator::read(Array *dst)
{
    TIMING("VariableDecorator::read(Array*)");
    return var->read(dst);
}


Array* VariableDecorator::read(int64_t record)
{
    TIMING("VariableDecorator::read(int64_t)");
    return var->read(record);
}


Array* VariableDecorator::read(int64_t record, Array *dst)
{
    TIMING("VariableDecorator::read(int64_t,Array*)");
    return var->read(record, dst);
}


Array* VariableDecorator::iread()
{
    TIMING("VariableDecorator::iread()");
    return var->iread();
}


Array* VariableDecorator::iread(Array *dst)
{
    TIMING("VariableDecorator::iread(Array*)");
    return var->iread(dst);
}


Array* VariableDecorator::iread(int64_t record)
{
    TIMING("VariableDecorator::iread(int64_t)");
    return var->iread(record);
}


Array* VariableDecorator::iread(int64_t record, Array *dst)
{
    TIMING("VariableDecorator::iread(int64_t,Array*)");
    return var->iread(record, dst);
}


ostream& VariableDecorator::print(ostream &os) const
{
    TIMING("VariableDecorator::print(ostream)");
    return var->print(os);
}
