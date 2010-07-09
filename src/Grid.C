#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using std::exception;
using std::ostream;
using std::string;
using std::vector;

#include "Grid.H"
#include "GeoGrid.H"


const GridType GridType::GEODESIC("geodesic");
int GridType::next_id(1);


GridType::GridType(const string &name)
    :   id(next_id++)
    ,   name(name)
{
}


GridType::GridType(const GridType &type)
    :   id(type.id)
    ,   name(type.name)
{
}


GridType& GridType::operator = (const GridType &type)
{
    if (this == &type) {
        return *this;
    }

    id = type.id;
    name = type.name;

    return *this;
}


bool GridType::operator == (const GridType &type) const
{
    return id == type.id;
}


bool GridType::operator != (const GridType &type) const
{
    return id != type.id;
}


string GridType::get_name() const
{
    return name;
}


ostream& operator << (ostream &os, const GridType &type)
{
    return os << type.get_name();
}


// needs improvement
vector<Grid*> Grid::get_grids(const Dataset *dataset)
{
    vector<Grid*> ret;
    vector<Grid*> grids;
    vector<Grid*>::iterator grid_it;

    grids = GeoGrid::get_grids(dataset);
    for (grid_it=grids.begin(); grid_it!=grids.end(); ++grid_it) {
        ret.push_back(*grid_it);
    }

    return ret;
}


Grid::Grid()
{
}


Grid::~Grid()
{
}
