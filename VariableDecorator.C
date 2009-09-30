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


size_t VariableDecorator::num_dims() const
{
    TIMING("VariableDecorator::num_dims()");
    return var->num_dims();
}


int64_t VariableDecorator::get_size(bool use_masks) const
{
    TIMING("VariableDecorator::get_size()");
    return var->get_size(use_masks);
}


vector<int64_t> VariableDecorator::get_sizes(bool use_masks) const
{
    TIMING("VariableDecorator::get_sizes()");
    return var->get_sizes(use_masks);
}


bool VariableDecorator::needs_subset() const
{
    TIMING("VariableDecorator::needs_subset()");
    return var->needs_subset();
}


vector<Attribute*> VariableDecorator::get_atts() const
{
    TIMING("VariableDecorator::get_atts()");
    return var->get_atts();
}


size_t VariableDecorator::num_atts() const
{
    TIMING("VariableDecorator::num_atts()");
    return var->num_atts();
}


string VariableDecorator::get_long_name() const
{
    TIMING("VariableDecorator::get_long_name()");
    return var->get_long_name();
}


Attribute* VariableDecorator::find_att(
        const string &name,
        bool ignore_case) const
{
    TIMING("VariableDecorator::find_att(string,bool)");
    return var->find_att(name, ignore_case);
}


Attribute* VariableDecorator::find_att(
        const vector<string> &names,
        bool ignore_case) const
{
    TIMING("VariableDecorator::find_att(vector<string>,bool)");
    return var->find_att(names, ignore_case);
}


DataType VariableDecorator::get_type() const
{
    TIMING("VariableDecorator::get_type()");
    return var->get_type();
}


int VariableDecorator::get_handle()
{
    TIMING("VariableDecorator::get_handle()");
    return var->get_handle();
}


void VariableDecorator::release_handle()
{
    TIMING("VariableDecorator::release_handle()");
    return var->release_handle();
}


void VariableDecorator::read()
{
    TIMING("VariableDecorator::read()");
    return var->read();
}


void VariableDecorator::set_record_index(size_t index)
{
    TIMING("VariableDecorator::set_record_index(size_t)");
    return var->set_record_index(index);
}


size_t VariableDecorator::get_record_index() const
{
    TIMING("VariableDecorator::get_record_index()");
    return var->get_record_index();
}


void VariableDecorator::reindex()
{
    TIMING("VariableDecorator::reindex()");
    return var->reindex();
}


ostream& VariableDecorator::print(ostream &os) const
{
    TIMING("VariableDecorator::print(ostream)");
    return var->print(os);
}
