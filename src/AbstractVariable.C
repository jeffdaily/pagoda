#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cassert>
#include <sstream>

#include "AbstractVariable.H"
#include "Array.H"
#include "Attribute.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Error.H"
#include "Grid.H"
#include "Mask.H"
#include "Pack.H"
#include "Print.H"
#include "StringComparator.H"
#include "ValidMax.H"
#include "ValidMin.H"
#include "ValidRange.H"
#include "Validator.H"
#include "Values.H"
#include "Variable.H"

using std::ostringstream;


AbstractVariable::AbstractVariable()
    :   Variable()
    ,   enable_record_translation(true)
{
}


AbstractVariable::~AbstractVariable()
{
}


bool AbstractVariable::has_record() const
{
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
    return get_dims().size();
}


vector<int64_t> AbstractVariable::get_shape() const
{
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
    return get_atts().size();
}


string AbstractVariable::get_standard_name() const
{
    string att_name("standard_name");
    Attribute *att = get_att(att_name);

    if (att) {
        return att->get_string();
    }

    return "";
}


string AbstractVariable::get_long_name() const
{
    string att_name("long_name");
    Attribute *att = get_att(att_name);

    if (att) {
        return att->get_string();
    }

    return "";
}


bool AbstractVariable::has_validator(int64_t index) const
{
    if (this->validator != NULL) {
        return true;
    } else {
        vector<string> names;
        names.push_back("_FillValue");
        names.push_back("missing_value");
        names.push_back("valid_min");
        names.push_back("valid_max");
        names.push_back("valid_range");
        return NULL != get_att(names);
    }
}


Validator* AbstractVariable::get_validator(int64_t index) const
{
    Validator *validator = NULL;
    DataType type_var = get_type();
    Attribute *att_valid_range = NULL;
    Attribute *att_valid_min = NULL;
    Attribute *att_valid_max = NULL;
    Attribute *att_fill_value = NULL;

    if (this->validator != NULL) {
        return this->validator;
    }

    att_valid_range = get_att("valid_range");
    att_valid_min = get_att("valid_min");
    att_valid_max = get_att("valid_max");
    att_fill_value = get_att("_FillValue");
    if (!att_fill_value) {
        att_fill_value = get_att("missing_value");
    }

    // look for newer attributes first
    if(att_valid_range) {
        bool ok = true;
        DataType type_att = att_valid_range->get_type();
        if (att_valid_min) {
            pagoda::print_zero("WARNING: both valid_min and valid_range");
            ok = false;
        }
        if (att_valid_max) {
            pagoda::print_zero("WARNING: both valid_max and valid_range");
            ok = false;
        }
        if (att_valid_range->get_count() != 2) {
            pagoda::print_zero("WARNING: valid_range incorrect count");
            ok = false;
        }
        if (type_att != type_var) {
            pagoda::print_zero("WARNING: valid_range type mismatch");
            ok = false;
        }
        if (ok) {
#define DATATYPE_EXPAND(DT,T) \
            if (type_att == DT) { \
                T min,max,fill; \
                if (get_fill_value(att_fill_value, fill)) { \
                    validator = new ValidRange<T>(min,max,fill); \
                } else { \
                    validator = new ValidRange<T>(min,max); \
                } \
            } else
#include "DataType.def"
            {
                ERR("DataType not handled");
            }
        }
    } else if (att_valid_min && att_valid_max) {
        bool ok = true;
        DataType type_att = att_valid_min->get_type();
        if (type_att != type_var) {
            pagoda::print_zero("WARNING: valid_min/var type mismatch");
            ok = false;
        }
        type_att = att_valid_max->get_type();
        if (type_att != type_var) {
            pagoda::print_zero("WARNING: valid_max/var type mismatch");
            ok = false;
        }
        if (type_att != att_valid_min->get_type()) {
            pagoda::print_zero("WARNING: valid_min/valid_max type mismatch");
            ok = false;
        }
        if (att_valid_min->get_count() != 1) {
            pagoda::print_zero("WARNING: valid_min incorrect count");
            ok = false;
        }
        if (att_valid_max->get_count() != 1) {
            pagoda::print_zero("WARNING: valid_max incorrect count");
            ok = false;
        }
        if (ok) {
#define DATATYPE_EXPAND(DT,T) \
            if (type_att == DT) { \
                T min,max,fill; \
                att_valid_min->get_values()->as(size_t(0), min); \
                att_valid_max->get_values()->as(size_t(0), max); \
                if (get_fill_value(att_fill_value, fill)) { \
                    validator = new ValidRange<T>(min,max,fill); \
                } else { \
                    validator = new ValidRange<T>(min,max); \
                } \
            } else
#include "DataType.def"
            {
                ERR("DataType not handled");
            }
        }
    } else if (att_valid_min) {
        bool ok = true;
        DataType type_att = att_valid_min->get_type();
        if (type_att != type_var) {
            pagoda::print_zero("WARNING: valid_min/var type mismatch");
            ok = false;
        }
        if (att_valid_min->get_count() != 1) {
            pagoda::print_zero("WARNING: valid_min incorrect count");
            ok = false;
        }
        if (ok) {
#define DATATYPE_EXPAND(DT,T) \
            if (type_att == DT) { \
                T min,fill; \
                att_valid_min->get_values()->as(size_t(0), min); \
                if (get_fill_value(att_fill_value, fill)) { \
                    validator = new ValidMin<T>(min,fill); \
                } else { \
                    validator = new ValidMin<T>(min); \
                } \
            } else
#include "DataType.def"
            {
                ERR("DataType not handled");
            }
        }
    } else if (att_valid_max) {
        bool ok = true;
        DataType type_att = att_valid_max->get_type();
        if (type_att != type_var) {
            pagoda::print_zero("WARNING: valid_max/var type mismatch");
            ok = false;
        }
        if (att_valid_max->get_count() != 1) {
            pagoda::print_zero("WARNING: valid_max incorrect count");
            ok = false;
        }
        if (ok) {
#define DATATYPE_EXPAND(DT,T) \
            if (type_att == DT) { \
                T max,fill; \
                att_valid_max->get_values()->as(size_t(0), max); \
                if (get_fill_value(att_fill_value, fill)) { \
                    validator = new ValidMax<T>(max,fill); \
                } else { \
                    validator = new ValidMax<T>(max); \
                } \
            } else
#include "DataType.def"
            {
                ERR("DataType not handled");
            }
        }
    } else if (att_fill_value) {
#define DATATYPE_EXPAND(DT,T) \
        if (type_var == DT) { \
            T fill; \
            if (get_fill_value(att_fill_value, fill)) { \
                if (fill > 0) { \
                    validator = new ValidMax<T>(fill,true); \
                } else { \
                    validator = new ValidMin<T>(fill,false); \
                } \
            } \
        } else
#include "DataType.def"
        {
            ERR("DataType not handled");
        }
    }

    return validator;
}


void AbstractVariable::set_validator(Validator *validator)
{
    this->validator = validator;
}


Attribute* AbstractVariable::get_att(
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


Attribute* AbstractVariable::get_att(
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

    if (masks.empty() || masks.at(0) == NULL || !enable_record_translation) {
        return record;
    }

    mask = masks.at(0);
    size = mask->get_size();
    buf = (int*)mask->get(0, size-1);

    for (index=0; index<size; ++index) {
        if (buf[index] != 0) {
            ++count;
            if (count == record) {
                break;
            }
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


static bool needs_subset_common(const vector<Mask*> &masks)
{
    vector<Mask*>::const_iterator mask_it;

    if (masks.empty()) {
        return false;
    }

    for (mask_it=masks.begin(); mask_it!=masks.end(); ++mask_it) {
        Mask *mask = *mask_it;
        if (mask) {
            int64_t count = mask->get_count();
            int64_t size = mask->get_size();
            if (count != size) {
                return true;
            }
        }
    }

    return false;
}


bool AbstractVariable::needs_subset() const
{
    return needs_subset_common(get_masks());
}


bool AbstractVariable::needs_subset_record() const
{
    vector<Mask*> masks = get_masks();
    if (!masks.empty()) {
        masks.erase(masks.begin());
    }
    return needs_subset_common(masks);
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
            if (mask) {
                int64_t count = mask->get_count();
                int64_t size = mask->get_size();
                if (count != size) {
                    return true;
                }
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
            if (mask) {
                pagoda::renumber(array, mask->reindex());
            }
            break;
        }
    }
}


Array* AbstractVariable::alloc(bool remove_record) const
{
    vector<int64_t> shape;
    Array *dst;

    shape = get_shape();
    if (remove_record) {
        shape.erase(shape.begin());
    }
    if (Variable::promote_to_float
            && get_type() != DataType::FLOAT
            && get_type() != DataType::DOUBLE) {
        dst = Array::create(DataType::DOUBLE, shape);
        // netcdf can't automatically convert char's to another type like it
        // can with other number types
        if (get_type() == DataType::CHAR) {
            dst->set_read_type(DataType::CHAR);
            dst->set_write_type(DataType::CHAR);
        }
    }
    else {
        dst = Array::create(get_type(), shape);
    }

    return dst;
}


Array* AbstractVariable::read_alloc() const
{
    return read(alloc(false));
}


Array* AbstractVariable::read_alloc(int64_t record) const
{
    return read(record, alloc(true));
}


Array* AbstractVariable::iread_alloc()
{
    return iread(alloc(false));
}


Array* AbstractVariable::iread_alloc(int64_t record)
{
    return iread(record, alloc(true));
}


ostream& AbstractVariable::print(ostream &os) const
{
    const string name(get_name());
    return os << "AbstractVariable(" << name << ")";
}


int64_t AbstractVariable::get_bytes() const
{
    int64_t bytes = pagoda::shape_to_size(get_shape()) * get_type().get_bytes();
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::iterator atts_it=atts.begin(), atts_end=atts.end();
    for (; atts_it!=atts_end; ++atts_it) {
        bytes += (*atts_it)->get_bytes();
    }
    return bytes;
}

