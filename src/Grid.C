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

#include "Attribute.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Error.H"
#include "Grid.H"
#include "GeoGrid.H"
#include "Util.H"
#include "Variable.H"


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


#include "Debug.H"
bool Grid::is_coordinate(const Variable *var)
{
    const Dataset *dataset = get_dataset();
    vector<Variable*> vars = dataset->get_vars();

    if (!var) {
        pagoda::print_zero("is_coordinate false null var\n");
        return false;
    }

    // does this Variable have only 1 dim with same name?
    if (var->get_dims().size() == 1
            && var->get_dims()[0]->get_name() == var->get_name()) {
        pagoda::print_zero("is_coordinate true dim name and size match\n");
        return true;
    }
    
    // does this Variable's name match any "coordinates" attribute?
    for (vector<Variable*>::iterator it=vars.begin(); it!=vars.end(); ++it) {
        Variable *current_var = *it;
        Attribute *att = current_var->get_att("coordinates");
        if (att) {
            vector<string> parts = pagoda::split(att->get_string());
            vector<string>::iterator part;
            for (part=parts.begin(); part!=parts.end(); ++part) {
                if (var->get_name() == *part) {
                    pagoda::print_zero(
                            "is_coordinate true coordiantes attribute\n");
                    return true;
                }
            }
        }
    }

    return false;
}


bool Grid::is_topology(const Variable *var)
{
    if (!var) {
        return false;
    }

    if (var == get_cell_cells()) {
        return true;
    }
    if (var == get_cell_edges()) {
        return true;
    }
    if (var == get_cell_corners()) {
        return true;
    }
    if (var == get_edge_cells()) {
        return true;
    }
    if (var == get_edge_edges()) {
        return true;
    }
    if (var == get_edge_corners()) {
        return true;
    }
    if (var == get_corner_cells()) {
        return true;
    }
    if (var == get_corner_edges()) {
        return true;
    }
    if (var == get_corner_corners()) {
        return true;
    }

    return false;
}


Dimension* Grid::get_topology_dim(const Variable *var)
{
    if (var == get_cell_cells()) {
        return get_cell_dim();
    }
    if (var == get_cell_edges()) {
        return get_edge_dim();
    }
    if (var == get_cell_corners()) {
        return get_corner_dim();
    }
    if (var == get_edge_cells()) {
        return get_cell_dim();
    }
    if (var == get_edge_edges()) {
        return get_edge_dim();
    }
    if (var == get_edge_corners()) {
        return get_corner_dim();
    }
    if (var == get_corner_cells()) {
        return get_cell_dim();
    }
    if (var == get_corner_edges()) {
        return get_edge_dim();
    }
    if (var == get_corner_corners()) {
        return get_corner_dim();
    }
    ERR("variable is not a topology variable");
}
