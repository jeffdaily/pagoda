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
        for ( ; it!=end; ++it) {
            shape.push_back((*it)->get_count());
        }
    } else {
        vector<Dimension*> dims = get_dims();
        vector<Dimension*>::const_iterator it = dims.begin();
        vector<Dimension*>::const_iterator end = dims.end();

        for ( ; it!=end; ++it) {
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

        for ( ; dim_it!=dims.end(); ++dim_it) {
            ret.push_back((*dim_it)->get_mask());
        }
    }

    return ret;
}


/**
 * If there is a Mask over the record Dimension, translate the given record
 * from the index space of the mask into the index space of the whole data.
 *
 * It is safe to call even if no masks are set in which case the record is
 * returned unchanged.
 *
 * @param[in] record the record to translate
 * @return the translated record
 */
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
        } else {
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


/**
 * Enables whether record translation takes place.
 *
 * @see translate_record
 */
void AbstractVariable::set_translate_record(bool value)
{
    enable_record_translation = value;
}


/**
 * Return true if this Variable has any active Masks.
 *
 * @return true if this Variable requires subsetting
 */
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


/**
 * Return true if this Variable has any active, non-record Masks.
 *
 * @return true if this Variable requires subsetting
 */
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


/**
 * If this is a topology Variable and its connected Dimension has been subset,
 * we must renumber this Variable's values.
 *
 * @return true if this Variable should be renumbered
 */
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


/**
 * If this is a topology Variable and its connected Dimension has been subset,
 * go ahead and renumber the given array assuming it was something that was
 * read earlier.
 *
 * @param[in] array the Array to renumber
 */
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


Array* AbstractVariable::read() const
{
    Array *dst = Array::create(get_type(), get_shape());
    return read(dst);
}


Array* AbstractVariable::read(int64_t record) const
{
    vector<int64_t> shape;
    Array *dst;

    shape = get_shape();
    shape.erase(shape.begin());
    dst = Array::create(get_type(), shape);

    return read(record, dst);
}


Array* AbstractVariable::iread()
{
    Array *dst = Array::create(get_type(), get_shape());
    return iread(dst);
}


Array* AbstractVariable::iread(int64_t record)
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
