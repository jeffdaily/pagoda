#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "AbstractDataset.H"
#include "Attribute.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Grid.H"
#include "MaskMap.H"
#include "StringComparator.H"
#include "Timing.H"
#include "Util.H"
#include "Variable.H"


AbstractDataset::AbstractDataset()
    :   Dataset()
    ,   grids()
    ,   masks()
{
    TIMING("AbstractDataset::AbstractDataset()");
}


AbstractDataset::~AbstractDataset()
{
    TIMING("AbstractDataset::~AbstractDataset()");
    transform(grids.begin(), grids.end(), grids.begin(),
              pagoda::ptr_deleter<Grid*>);
}


vector<Grid*> AbstractDataset::get_grids()
{
    if (grids.empty()) {
        grids = Grid::get_grids(this);
    }
    return grids;
}


Grid* AbstractDataset::get_grid()
{
    vector<Grid*> grids = get_grids();
    if (!grids.empty()) {
        return grids[0];
    }
    return NULL;
}


Attribute* AbstractDataset::get_att(
    const string &name, bool ignore_case, bool within) const
{
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::const_iterator it = atts.begin();
    vector<Attribute*>::const_iterator end = atts.end();
    StringComparator cmp;

    TIMING("AbstractDataset::get_att(string,bool,bool)");

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(name)) {
            return *it;
        }
    }

    return NULL;
}


Attribute* AbstractDataset::get_att(
    const vector<string> &names, bool ignore_case, bool within) const
{
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::const_iterator it = atts.begin();
    vector<Attribute*>::const_iterator end = atts.end();
    StringComparator cmp;

    TIMING("AbstractDataset::get_att(vector<string>,bool,bool)");

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(names)) {
            return *it;
        }
    }

    return NULL;
}


Dimension* AbstractDataset::get_dim(
    const string &name, bool ignore_case, bool within) const
{
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::const_iterator it = dims.begin();
    vector<Dimension*>::const_iterator end = dims.end();
    StringComparator cmp;

    TIMING("AbstractDataset::get_dim(string,bool,bool)");

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(name)) {
            return *it;
        }
    }

    return NULL;
}


Dimension* AbstractDataset::get_udim() const
{
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::const_iterator it = dims.begin();
    vector<Dimension*>::const_iterator end = dims.end();

    for (; it!=end; ++it) {
        if ((*it)->is_unlimited()) {
            return *it;
        }
    }

    return NULL;
}


Variable* AbstractDataset::get_var(
    const string &name, bool ignore_case, bool within) const
{
    TIMING("AbstractDataset::get_var(string,bool,bool)");
    vector<Variable*> vars = get_vars();
    vector<Variable*>::const_iterator it = vars.begin();
    vector<Variable*>::const_iterator end = vars.end();
    StringComparator cmp;

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(name)) {
            return *it;
        }
    }

    return NULL;
}


void AbstractDataset::set_masks(MaskMap *masks)
{
    this->masks.assign(1,masks);
}


void AbstractDataset::push_masks(MaskMap *masks)
{
    this->masks.push_back(masks);
}


MaskMap* AbstractDataset::pop_masks()
{
    if (masks.empty()) {
        return NULL;
    }
    else {
        MaskMap *ret = masks.back();
        masks.pop_back();
        return ret;
    }
}


MaskMap* AbstractDataset::get_masks() const
{
    if (masks.empty()) {
        return NULL;
    }
    else {
        return masks.back();
    }
}


int64_t AbstractDataset::get_bytes() const
{
    int64_t bytes = 0;
    vector<Variable*> vars = get_vars();
    vector<Variable*>::const_iterator vars_it = vars.begin();
    vector<Variable*>::const_iterator vars_end = vars.end();
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::const_iterator atts_it = atts.begin();
    vector<Attribute*>::const_iterator atts_end = atts.end();

    for (; vars_it!=vars_end; ++vars_it) {
        bytes += (*vars_it)->get_bytes();
    }
    for (; atts_it!=atts_end; ++atts_it) {
        bytes += (*atts_it)->get_bytes();
    }

    return bytes;
}

