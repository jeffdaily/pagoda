#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Dataset.H"
#include "Dimension.H"
#include "Mask.H"
#include "MaskMap.H"


Dimension::Dimension()
{
}


Dimension::Dimension(const Dimension &copy)
{
}


Dimension& Dimension::operator = (const Dimension &copy)
{
    if (&copy != this) {
    }
    return *this;
}


Dimension::~Dimension()
{
}


Mask* Dimension::get_mask() const
{
    Dataset *dataset = get_dataset();
    MaskMap *masks = dataset->get_masks();
    if (masks && masks->has_mask(this)) {
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


bool Dimension::conforms(
        const vector<Dimension*> &left, const vector<Dimension*> &right)
{
    if (left.size() < right.size()) {
        return false;
    }
 
    // set intersection assuming same ordering of vectors
    size_t index_left=0, limit_left=left.size();
    size_t index_right=0, limit_right=right.size();
    size_t count=0;
    while (index_left < limit_left && index_right < limit_right) {
        if (equal(left[index_left], right[index_right])) {
            ++count;
            ++index_left;
            ++index_right;
        }
        else {
            ++index_left;
        }
    }

    return count == limit_right;
}


ostream& operator << (ostream &os, const Dimension *other)
{
    return other->print(os);
}
