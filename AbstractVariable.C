#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <sstream>

#include <ga.h>

#include "AbstractVariable.H"
#include "Attribute.H"
#include "Dimension.H"
#include "Mask.H"
#include "StringComparator.H"

using std::ostringstream;


AbstractVariable::AbstractVariable()
    :   Variable()
    ,   handle(NULL)
    ,   record_index(0)
{
}


AbstractVariable::~AbstractVariable()
{
    release_handle();
}


bool AbstractVariable::has_record() const
{
    return get_dims()[0]->is_unlimited();
}


size_t AbstractVariable::num_dims() const
{
    return get_dims().size();
}


int64_t AbstractVariable::get_size(bool use_masks) const
{
    int64_t result = 1;
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::const_iterator it = dims.begin();
    vector<Dimension*>::const_iterator end = dims.end();

    for (; it!=end; ++it) {
        if (! (*it)->is_unlimited()) {
            Mask *mask = (*it)->get_mask();
            if (use_masks && mask) {
                result *= mask->get_count();
            } else {
                result *= (*it)->get_size();
            }
        }
    }

    return result;
}


vector<int64_t> AbstractVariable::get_sizes(bool use_masks) const
{
    vector<int64_t> sizes;
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::const_iterator it = dims.begin();
    vector<Dimension*>::const_iterator end = dims.end();

    for (; it!=end; ++it) {
        Mask *mask = (*it)->get_mask();
        int64_t size;
        if (use_masks && mask) {
            size = mask->get_count();
        } else {
            size = (*it)->get_size();
        }
        sizes.push_back(size);
    }

    return sizes;
}


bool AbstractVariable::needs_subset() const
{
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::const_iterator it = dims.begin();
    vector<Dimension*>::const_iterator end = dims.end();

    for (; it!=end; ++it) {
        Dimension *dim = *it;
        Mask *mask = dim->get_mask();;
        if (mask) {
            if (mask->get_size() != mask->get_count()) {
                return true;
            }
        }
    }

    return false;
}


size_t AbstractVariable::num_atts() const
{
    return get_atts().size();
}


string AbstractVariable::get_long_name() const
{
    string att_name("long_name");
    Attribute *att = find_att(att_name);

    if (att) {
        ostringstream val;
        val << att->get_values();
        return val.str();
    }
    return "";
}


Attribute* AbstractVariable::find_att(
        const string &name,
        bool ignore_case) const
{
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


Attribute* AbstractVariable::find_att(
        const vector<string> &names,
        bool ignore_case) const
{
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


int AbstractVariable::get_handle()
{
    if (! handle) {
        vector<int64_t> dim_sizes = get_sizes();
        char *name = const_cast<char*>(get_name().c_str());
        int64_t *size_tmp; // because NGA_Create64 expects int64_t pointer
        int ndim;
        int tmp_handle;
        if (has_record() && num_dims() > 1) {
            size_tmp = &dim_sizes[1];
            ndim = num_dims() - 1;
        } else {
            size_tmp = &dim_sizes[0];
            ndim = num_dims();
        }
        tmp_handle = NGA_Create64(get_type(), ndim, size_tmp, name, NULL);
        handle = new int(tmp_handle);
    }
    return *handle;
}


void AbstractVariable::release_handle()
{
    if (handle) {
        GA_Destroy(*handle);
        delete handle;
        handle = NULL;
    }
}


void AbstractVariable::set_record_index(size_t index)
{
    record_index = index;
}


size_t AbstractVariable::get_record_index() const
{
    return record_index;
}


void AbstractVariable::reindex()
{
}


ostream& AbstractVariable::print(ostream &os) const
{
    const string name(get_name());
    return os << "AbstractVariable(" << name << ")";
}

