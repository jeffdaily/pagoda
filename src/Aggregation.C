#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "AbstractDataset.H"
#include "Aggregation.H"
#include "Dataset.H"
#include "Timing.H"
#include "Util.H"


Aggregation::Aggregation()
    :   AbstractDataset()
    ,   datasets()
    ,   atts()
    ,   dims()
    ,   vars()
{
}


Aggregation::~Aggregation()
{
    // before deleting aggregate datasets, remove their (shared) masks
    // since all MaskMaps get deleted with the Dataset
    for (vector<Dataset*>::iterator it=datasets.begin(), end=datasets.end();
            it!=end; ++it) {
        Dataset *dataset = *it;
        dataset->set_masks(NULL);
        delete dataset;
    }
}


void Aggregation::close()
{
    vector<Dataset*> datasets = get_datasets();
    vector<Dataset*>::iterator it;

    for (it=datasets.begin(); it!=datasets.end(); ++it) {
        (*it)->close();
    }
}


void Aggregation::add(const vector<Dataset*> &datasets)
{
    vector<Dataset*>::const_iterator it;
    for (it=datasets.begin(); it!=datasets.end(); ++it) {
        add(*it);
    }
}


vector<Attribute*> Aggregation::get_atts() const
{
    return atts;
}


vector<Dimension*> Aggregation::get_dims() const
{
    return dims;
}


vector<Variable*> Aggregation::get_vars() const
{
    return vars;
}


vector<Dataset*> Aggregation::get_datasets() const
{
    return datasets;
}


void Aggregation::wait()
{
    for (vector<Dataset*>::iterator it=datasets.begin(), end=datasets.end();
            it!=end; ++it) {
        (*it)->wait();
    }
}


FileFormat Aggregation::get_file_format() const
{
    if (datasets.empty()) {
        return FF_UNKNOWN;
    }
    else {
        return datasets.front()->get_file_format();
    }
}


void Aggregation::set_masks(MaskMap *masks)
{
    vector<Dataset*> datasets = get_datasets();
    vector<Dataset*>::iterator it;

    for (it=datasets.begin(); it!=datasets.end(); ++it) {
        (*it)->set_masks(masks);
    }

    AbstractDataset::set_masks(masks);
}


void Aggregation::push_masks(MaskMap *masks)
{
    vector<Dataset*> datasets = get_datasets();
    vector<Dataset*>::iterator it;

    for (it=datasets.begin(); it!=datasets.end(); ++it) {
        (*it)->push_masks(masks);
    }

    AbstractDataset::push_masks(masks);
}


MaskMap* Aggregation::pop_masks()
{
    vector<Dataset*> datasets = get_datasets();
    vector<Dataset*>::iterator it;

    for (it=datasets.begin(); it!=datasets.end(); ++it) {
        (*it)->pop_masks();
    }

    return AbstractDataset::pop_masks();
}
