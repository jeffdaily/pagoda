#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Dataset.H"
#include "Dimension.H"
#include "PnetcdfDataset.H"
#include "Timing.H"
#include "Util.H"
#include "Variable.H"


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


bool Dataset::equal(const Dataset *left, const Dataset *right)
{
    vector<Dimension*> left_dims = left->get_dims();
    vector<Dimension*> right_dims = right->get_dims();
    vector<Variable*> left_vars = left->get_vars();
    vector<Variable*> right_vars = right->get_vars();

    if (Dimension::equal(left_dims,right_dims)
            && Variable::equal(left_vars,right_vars)) {
        return true;
    }

    return false;
}
