#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <sstream>

#include "AbstractVariable.H"
#include "Array.H"
#include "Attribute.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "Error.H"
#include "Grid.H"
#include "Mask.H"
#include "Pack.H"
#include "StringComparator.H"
#include "Timing.H"
#include "Values.H"
#include "Variable.H"

using std::ostringstream;


AbstractVariable::AbstractVariable()
    :   Variable()
    ,   enable_record_translation(true)
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
    vector<Dimension*> dims = get_dims();
    if (dims.empty()) {
        return false;
    }
    return dims[0]->is_unlimited();
}


int64_t AbstractVariable::get_nrec() const
{
    return get_shape().at(0);
}


int64_t AbstractVariable::get_ndim() const
{
    TIMING("AbstractVariable::get_ndim()");
    return get_dims().size();
}


vector<int64_t> AbstractVariable::get_shape() const
{
    vector<int64_t> shape;

    TIMING("AbstractVariable::get_shape(bool)");

    if (get_dataset()->get_masks()) {
        vector<Mask*> masks = get_masks();
        vector<Mask*>::const_iterator it = masks.begin();
        vector<Mask*>::const_iterator end = masks.end();
        for (; it!=end; ++it) {
            shape.push_back((*it)->get_count());
        }
    }
    else {
        vector<Dimension*> dims = get_dims();
        vector<Dimension*>::const_iterator it = dims.begin();
        vector<Dimension*>::const_iterator end = dims.end();

        for (; it!=end; ++it) {
            shape.push_back((*it)->get_size());
        }
    }

    return shape;
}


int64_t AbstractVariable::num_atts() const
{
    TIMING("AbstractVariable::num_atts()");
    return get_atts().size();
}


string AbstractVariable::get_standard_name() const
{
    TIMING("AbstractVariable::get_standard_name()");
    string att_name("standard_name");
    Attribute *att = get_att(att_name);

    if (att) {
        return att->get_string();
    }

    return "";
}


string AbstractVariable::get_long_name() const
{
    TIMING("AbstractVariable::get_long_name()");
    string att_name("long_name");
    Attribute *att = get_att(att_name);

    if (att) {
        return att->get_string();
    }

    return "";
}


bool AbstractVariable::has_fill_value() const
{
    vector<string> names;
    names.push_back("_FillValue");
    names.push_back("missing_value");
    return NULL != get_att(names);
}


double AbstractVariable::get_fill_value() const
{
    double value;
    Attribute *att;
    vector<string> names;

    names.push_back("_FillValue");
    names.push_back("missing_value");
    att = get_att(names);
    if (NULL == att) {
        ERR("no _FillValue nor missing_value attribute\n"
                "code should test has_fill_value() first");
    }
    if (att->get_type() == DataType::CHAR) {
        istringstream sin(att->get_string());
        sin >> value;
        if (sin.fail()) {
            ERR("could not convert _FillValue/missing_value from char");
        }
    } else {
        att->get_values()->as(0, value);
    }

    return value;
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


vector<Mask*> AbstractVariable::get_masks() const
{
    vector<Mask*> ret;

    if (get_dataset()->get_masks()) {
        vector<Dimension*> dims = get_dims();
        vector<Dimension*>::iterator dim_it = dims.begin();

        for (; dim_it!=dims.end(); ++dim_it) {
            ret.push_back((*dim_it)->get_mask());
        }
    }

    return ret;
}


int64_t AbstractVariable::translate_record(int64_t record) const
{
    vector<Mask*> masks = get_masks();
    Mask *mask = NULL;
    int64_t size = -1;
    int *buf = NULL;
    int64_t count = -1;
    int64_t index = 0;

    TRACER("AbstractVariable::translate_record(%ld) %s\n",
           record, get_name().c_str());
    TRACER("size=%ld, count=%ld, index=%ld\n", size, count, index);

    if (masks.empty() || !enable_record_translation) {
        return record;
    }

    mask = masks.at(0);
    size = mask->get_size();
    buf = (int*)mask->get(0, size-1);

    for (index=0; index<size; ++index) {
        if (buf[index] != 0) {
            ++count;
            TRACER("buf[index]=%ld, count=%ld, index=%ld\n",
                   buf[index], count, index);
            if (count == record) {
                break;
            }
        }
        else {
            TRACER("buf[index]=%ld, count=%ld, index=%ld\n",
                   buf[index], count, index);
        }
    }
    delete [] buf;
    if (index >= size) {
        ERR("index overrun");
    }

    return index;
}


void AbstractVariable::set_translate_record(bool value)
{
    enable_record_translation = value;
}


bool AbstractVariable::needs_subset() const
{
    vector<Mask*> masks = get_masks();
    vector<Mask*>::iterator mask_it;

    if (masks.empty()) {
        return false;
    }

    for (mask_it=masks.begin(); mask_it!=masks.end(); ++mask_it) {
        int64_t count = (*mask_it)->get_count();
        int64_t size = (*mask_it)->get_size();
        if (count != size) {
            return true;
        }
    }

    return false;
}


bool AbstractVariable::needs_subset_record() const
{
    vector<Mask*> masks = get_masks();
    vector<Mask*>::iterator mask_it;

    if (masks.empty()) {
        return false;
    }

    masks.erase(masks.begin());

    for (mask_it=masks.begin(); mask_it!=masks.end(); ++mask_it) {
        int64_t count = (*mask_it)->get_count();
        int64_t size = (*mask_it)->get_size();
        if (count != size) {
            return true;
        }
    }

    return false;
}


bool AbstractVariable::needs_renumber() const
{
    vector<Grid*> grids = get_dataset()->get_grids();
    vector<Grid*>::iterator grid_it;

    for (grid_it=grids.begin(); grid_it!=grids.end(); ++grid_it) {
        Grid *grid = *grid_it;
        if (grid->is_topology(this)) {
            Dimension *dim = grid->get_topology_dim(this);
            Mask *mask = dim->get_mask();
            int64_t count = mask->get_count();
            int64_t size = mask->get_size();
            if (count != size) {
                return true;
            }
        }
    }

    return false;
}


void AbstractVariable::renumber(Array *array) const
{
    vector<Grid*> grids = get_dataset()->get_grids();
    vector<Grid*>::iterator grid_it;

    for (grid_it=grids.begin(); grid_it!=grids.end(); ++grid_it) {
        Grid *grid = *grid_it;
        if (grid->is_topology(this)) {
            Dimension *dim = grid->get_topology_dim(this);
            Mask *mask = dim->get_mask();
            pagoda::renumber(array, mask->reindex());
            break;
        }
    }
}


Array* AbstractVariable::read_alloc() const
{
    Array *dst = Array::create(get_type(), get_shape());
    return read(dst);
}


Array* AbstractVariable::read_alloc(int64_t record) const
{
    vector<int64_t> shape;
    Array *dst;

    shape = get_shape();
    shape.erase(shape.begin());
    dst = Array::create(get_type(), shape);

    return read(record, dst);
}


Array* AbstractVariable::iread_alloc()
{
    Array *dst = Array::create(get_type(), get_shape());
    return iread(dst);
}


Array* AbstractVariable::iread_alloc(int64_t record)
{
    vector<int64_t> shape;
    Array *dst;

    shape = get_shape();
    shape.erase(shape.begin());
    dst = Array::create(get_type(), shape);

    return iread(record, dst);
}


ostream& AbstractVariable::print(ostream &os) const
{
    TIMING("AbstractVariable::print(ostream)");
    const string name(get_name());
    return os << "AbstractVariable(" << name << ")";
}
