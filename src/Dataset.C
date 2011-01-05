#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <vector>

#include "Dataset.H"
#include "Dimension.H"
#include "Error.H"
#include "Timing.H"
#include "Util.H"
#include "Variable.H"

using std::vector;


vector<Dataset::opener_t> Dataset::openers;


Dataset* Dataset::open(const string &filename)
{
    Dataset *dataset = NULL;
    vector<opener_t>::iterator it = Dataset::openers.begin();
    vector<opener_t>::iterator end = Dataset::openers.end();

    for (; it != end; ++it) {
        dataset = (*it)(filename);
        if (NULL != dataset) {
            break;
        }
    }
    if (NULL == dataset) {
        ERR("unable to open dataset -- no handler found");
    }

    return dataset;
}


void Dataset::register_opener(opener_t opener)
{
    openers.push_back(opener);
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
