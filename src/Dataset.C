#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Dataset.H"
#include "PnetcdfDataset.H"
#include "Timing.H"
#include "Util.H"


Dataset* Dataset::open(const string &filename)
{
    //TIMING("Dataset::open(string)");
    Dataset *dataset = NULL;
    string EXT_NC(".nc");
    if (pagoda::ends_with(filename, EXT_NC)) {
        dataset = new PnetcdfDataset(filename);
    }
    return dataset;
}


Dataset::Dataset()
{
    TIMING("Dataset::Dataset()");
}


Dataset::~Dataset()
{
    TIMING("Dataset::~Dataset()");
}


ostream& operator << (ostream &os, const Dataset *dataset)
{
    return dataset->print(os);
}
