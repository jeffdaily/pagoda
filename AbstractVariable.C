#include "Attribute.H"
#include "Dimension.H"
#include "AbstractVariable.H"
#include "Util.H"


AbstractVariable::AbstractVariable()
    :   Variable()
    ,   handle(0)
    ,   record_index(0)
{
}


AbstractVariable::~AbstractVariable()
{
}


int64_t AbstractVariable::get_size() const
{
    int64_t result = 1;
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::const_iterator it;

    for (it=dims.begin(); it!=dims.end(); ++it) {
        if (! (*it)->is_unlimited())
            result *= (*it)->get_size();
    }

    return result;
}


int64_t* AbstractVariable::get_sizes() const
{
    size_t dimidx;
    size_t ndim = num_dims();
    int64_t *sizes = new int64_t[ndim];
    vector<Dimension*> dims = get_dims();

    for (dimidx=0; dimidx<ndim; ++dimidx) {
        sizes[dimidx] = dims[dimidx]->get_size();
    }

    return sizes;
}


size_t AbstractVariable::num_masks() const
{
    size_t result = 0;
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::const_iterator it;

    for (it=dims.begin(); it!=dims.end(); ++it) {
        if (((*it)->get_mask())) {
            ++result;
        }
    }

    return result;
}


string AbstractVariable::get_long_name() const
{
    Attribute *att = find_att("long_name");
    if (att) {
        ostringstream val;
        val << att->get_values();
        return val.str();
    }
    return "";
}


Attribute* AbstractVariable::find_att(const string &name, bool ignore_case) const
{
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::iterator result;
    result = Util::find(atts, name, ignore_case);
    if (result != atts.end())
        return *result;
    return NULL;
}


Attribute* AbstractVariable::find_att(
        const vector<string> &names,
        bool ignore_case) const
{
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::iterator result;
    result = Util::find(atts, names, ignore_case);
    if (result != atts.end())
        return *result;
    return NULL;
}


int AbstractVariable::get_handle()
{
    if (handle == 0) {
        int64_t *dim_sizes = get_sizes();
        handle = NGA_Create64(get_type().as_mt(), num_dims(), dim_sizes,
            (char*)get_name().c_str(), NULL);
        delete [] dim_sizes;
    }
    return handle;
}


void AbstractVariable::release_handle()
{
    GA_Destroy(handle);
    handle = 0;
}


void AbstractVariable::set_record_index(size_t index)
{
    record_index = index;
}


size_t AbstractVariable::get_record_index() const
{
    return record_index;
}


ostream& AbstractVariable::print(ostream &os) const
{
    return os << "AbstractVariable(" << get_name() << ")";
}

