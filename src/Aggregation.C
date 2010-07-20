#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "AbstractDataset.H"
#include "Aggregation.H"
#include "Dataset.H"
#include "Timing.H"


Aggregation::Aggregation()
    :   AbstractDataset()
{
    TIMING("Aggregation::Aggregation()");
}


Aggregation::~Aggregation()
{
    TIMING("Aggregation::~Aggregation()");
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
    TIMING("Aggregation::add(vector<Dataset*>)");
    vector<Dataset*>::const_iterator it;
    for (it=datasets.begin(); it!=datasets.end(); ++it) {
        add(*it);
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
