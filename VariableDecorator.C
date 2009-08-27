#include "VariableDecorator.H"

VariableDecorator::VariableDecorator(Variable *var)
    :   Variable()
    ,   var(var)
{
}


VariableDecorator::~VariableDecorator()
{
    delete var;
    var = NULL;
}


string VariableDecorator::get_name() const
{
    return var->get_name();
}


bool VariableDecorator::has_record() const
{
    return var->has_record();
}


size_t VariableDecorator::num_dims() const
{
    return var->num_dims();
}


vector<Dimension*> VariableDecorator::get_dims() const
{
    return var->get_dims();
}


int64_t VariableDecorator::get_size(const bool &use_masks) const
{
    return var->get_size(use_masks);
}


int64_t* VariableDecorator::get_sizes(const bool &use_masks) const
{
    return var->get_sizes(use_masks);
}


size_t VariableDecorator::num_masks() const
{
    return var->num_masks();
}


size_t VariableDecorator::num_cleared_masks() const
{
    return var->num_cleared_masks();
}


size_t VariableDecorator::num_atts() const
{
    return var->num_atts();
}


vector<Attribute*> VariableDecorator::get_atts() const
{
    return var->get_atts();
}


string VariableDecorator::get_long_name() const
{
    return var->get_long_name();
}


Attribute* VariableDecorator::find_att(
        const string &name,
        bool ignore_case) const
{
    return var->find_att(name, ignore_case);
}


Attribute* VariableDecorator::find_att(
        const vector<string> &names,
        bool ignore_case) const
{
    return var->find_att(names, ignore_case);
}


DataType VariableDecorator::get_type() const
{
    return var->get_type();
}


int VariableDecorator::get_handle()
{
    return var->get_handle();
}


void VariableDecorator::release_handle()
{
    return var->release_handle();
}


void VariableDecorator::read()
{
    return var->read();
}


void VariableDecorator::set_record_index(size_t index)
{
    return var->set_record_index(index);
}


size_t VariableDecorator::get_record_index() const
{
    return var->get_record_index();
}


void VariableDecorator::reindex()
{
    return var->reindex();
}


ostream& VariableDecorator::print(ostream &os) const
{
    return var->print(os);
}

