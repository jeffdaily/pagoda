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
#include "RegularGrid.H"
#include "Util.H"
#include "Variable.H"


int GridType::next_id(1);
const GridType GridType::GEODESIC("geodesic");
const GridType GridType::REGULAR("regular");


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
    grids = RegularGrid::get_grids(dataset);
    for (grid_it=grids.begin(); grid_it!=grids.end(); ++grid_it) {
        ret.push_back(*grid_it);
    }
    // we assume a RegularGrid if no grid was found by some other means
    if (ret.empty()) {
        ret.push_back(new RegularGrid(dataset));
    }

    return ret;
}


Grid::Grid()
{
}


Grid::~Grid()
{
}


/**
 * Return the "dummy" grid Variable, if any.
 *
 * This is the scalar Variable meant to contain relevant grid metadata only.
 *
 * @return the Variable, NULL if not found
 */
Variable* Grid::get_grid() const
{
    const Dataset *dataset = get_dataset();
    vector<Variable*> vars = dataset->get_vars();
    vector<Variable*>::iterator var_it;

    // look for a Variable with a standard_name of "grid" or cell_type att
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        if (var->get_standard_name() == "grid"
                || var->get_att("cell_type")) {
            return var;
        }
    }

    return NULL;
}


bool Grid::is_coordinate(const Variable *var)
{
    bool ret = false;;


    // check grid Variables first
    if (!var) {
        ret = false;
    }
    // these hard-coded names come from how NCO handles special CF variables
    else if (var->get_name() == "ORO") {
        ret = true;
    }
    else if (var->get_name() == "area") {
        ret = true;
    }
    else if (var->get_name() == "datesec") {
        ret = true;
    }
    else if (var->get_name() == "date") {
        ret = true;
    }
    else if (var->get_name() == "gw") {
        ret = true;
    }
    else if (var->get_name() == "hyai") {
        ret = true;
    }
    else if (var->get_name() == "hyam") {
        ret = true;
    }
    else if (var->get_name() == "hybi") {
        ret = true;
    }
    else if (var->get_name() == "hybm") {
        ret = true;
    }
    else if (var->get_name() == "lat_bnds") {
        ret = true;
    }
    else if (var->get_name() == "lon_bnds") {
        ret = true;
    }
    else if (pagoda::starts_with(var->get_name(), "msk_")) {
        ret = true;
    }
    // now for the non-hardcoded checks
    else if (var == get_cell_lat()) {
        ret = true;
    }
    else if (var == get_cell_lon()) {
        ret = true;
    }
    else if (var == get_edge_lat()) {
        ret = true;
    }
    else if (var == get_edge_lon()) {
        ret = true;
    }
    else if (var == get_corner_lat()) {
        ret = true;
    }
    else if (var == get_corner_lon()) {
        ret = true;
    }
    else if (var->get_dims().size() == 1
             && var->get_dims()[0]->get_name() == var->get_name()) {
        // does this Variable have only 1 dim with same name?
        ret = true;
    }
    else {
        // does this Variable's name match any "coordinates" attribute?
        // does this Variable's name match any "bounds" attribute?
        const Dataset *dataset = get_dataset();
        vector<Variable*> vars = dataset->get_vars();
        vector<Variable*>::iterator it;
        for (it=vars.begin(); it!=vars.end(); ++it) {
            Variable *current_var = *it;
            Attribute *att;
            string name = var->get_name();

            if ((att = current_var->get_att("coordinates"))) {
                vector<string> parts = pagoda::split(att->get_string());
                vector<string>::iterator part;
                for (part=parts.begin(); part!=parts.end(); ++part) {
                    if (name == *part) {
                        ret = true;
                        break;
                    }
                }
            }
            if (ret) {
                break;
            }
            if ((att = current_var->get_att("bounds"))) {
                string bounds = att->get_string();
                vector<string> parts = pagoda::split(bounds);
                vector<string>::iterator part;
                for (part=parts.begin(); part!=parts.end(); ++part) {
                    if (name == *part) {
                        ret = true;
                        break;
                    }
                }
            }
            if (ret) {
                break;
            }
        }
    }

    return ret;
}


bool Grid::is_topology(const Variable *var)
{
    bool ret;

    if (!var) {
        ret = false;
    }
    else if (var == get_cell_cells()) {
        ret = true;
    }
    else if (var == get_cell_edges()) {
        ret = true;
    }
    else if (var == get_cell_corners()) {
        ret = true;
    }
    else if (var == get_edge_cells()) {
        ret = true;
    }
    else if (var == get_edge_edges()) {
        ret = true;
    }
    else if (var == get_edge_corners()) {
        ret = true;
    }
    else if (var == get_corner_cells()) {
        ret = true;
    }
    else if (var == get_corner_edges()) {
        ret = true;
    }
    else if (var == get_corner_corners()) {
        ret = true;
    }
    else if (var == get_grid()) {
        ret = true;
    }
    else {
        ret = false;
    }

#ifdef TRACE
    if (ret) {
    }
    else {
    }
#endif

    return ret;
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
