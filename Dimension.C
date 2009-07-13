#include "Dimension.H"

Dimension::Dimension()
    :   mask(NULL)
{
}


Dimension::Dimension(const Dimension &copy)
    :   mask(copy.mask)
{
}


Dimension& Dimension::operator = (const Dimension &copy)
{
    if (&copy != this) {
        mask = copy.mask;
    }
    return *this;
}


Dimension::~Dimension()
{
}


void Dimension::set_mask(Mask *mask)
{
    this->mask = mask;
}


Mask* Dimension::get_mask() const
{
    return mask;
}


ostream& operator << (ostream &os, const Dimension *other)
{
    return other->print(os);
}

