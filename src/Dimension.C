#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Dataset.H"
#include "Dimension.H"
#include "Mask.H"
#include "MaskMap.H"
#include "Timing.H"


Dimension::Dimension()
{
    TIMING("Dimension::Dimension()");
}


Dimension::Dimension(const Dimension &copy)
{
    TIMING("Dimension::Dimension(Dimension)");
}


Dimension& Dimension::operator = (const Dimension &copy)
{
    TIMING("Dimension::operator=(Dimension)");
    if (&copy != this) {
    }
    return *this;
}


Dimension::~Dimension()
{
    TIMING("Dimension::~Dimension()");
}


Mask* Dimension::get_mask() const
{
    Dataset *dataset = get_dataset();
    MaskMap *masks = dataset->get_masks();
    TIMING("Dimension::get_mask()");
    if (masks) {
        return masks->get_mask(this);
    }
    else {
        return NULL;
    }
}


bool Dimension::equal(const Dimension *left, const Dimension *right)
{
    return left->get_size() == right->get_size()
           && left->get_name() == right->get_name();
}


bool Dimension::equal(
    const vector<Dimension*> &left, const vector<Dimension*> &right)
{
    if (left.size() != right.size()) {
        return false;
    }
    for (size_t i=0; i<left.size(); ++i) {
        size_t j;
        Dimension *left_dim = left[i];
        for (j=0; j<right.size(); j++) {
            if (Dimension::equal(left_dim, right[j])) {
                break;
            }
        }
        if (j >= right.size()) {
            return false;
        }
    }

    return true;
}


ostream& operator << (ostream &os, const Dimension *other)
{
    TIMING("operator<<(ostream,Dimension*)");
    return other->print(os);
}
