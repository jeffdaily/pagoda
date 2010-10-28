#if HAVE_CONFIG_H
#   include "config.h"
#endif

#include "GenericAttribute.H"
#include "Values.H"

GenericAttribute::GenericAttribute(
    const string &name, Values *values, DataType type)
    :   Attribute()
    ,   name(name)
    ,   values(values)
    ,   type(type)
{
}


GenericAttribute::~GenericAttribute()
{
    delete values;
}


string GenericAttribute::get_name() const
{
    return name;
}


DataType GenericAttribute::get_type() const
{
    return type;
}


Values* GenericAttribute::get_values() const
{
    return values;
}


Dataset* GenericAttribute::get_dataset() const
{
    return NULL;
}


Variable* GenericAttribute::get_var() const
{
    return NULL;
}


ostream& GenericAttribute::print(ostream &os) const
{
    os << "GenericAttribute(" << name << ",...," << type << ")";
    return os;
}
