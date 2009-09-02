#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Aggregation.H"
#include "Dataset.H"


Aggregation::Aggregation()
    :   Dataset()
{
}


Aggregation::~Aggregation()
{
}


void Aggregation::add(const vector<Dataset*> &datasets)
{
    vector<Dataset*>::const_iterator it;
    for (it=datasets.begin(); it!=datasets.end(); ++it) {
        add(*it);
    }
}
