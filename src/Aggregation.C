#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Aggregation.H"
#include "Dataset.H"
#include "Timing.H"


Aggregation::Aggregation()
    :   Dataset()
{
    TIMING("Aggregation::Aggregation()");
}


Aggregation::~Aggregation()
{
    TIMING("Aggregation::~Aggregation()");
}


void Aggregation::add(const vector<Dataset*> &datasets)
{
    TIMING("Aggregation::add(vector<Dataset*>)");
    vector<Dataset*>::const_iterator it;
    for (it=datasets.begin(); it!=datasets.end(); ++it) {
        add(*it);
    }
}
