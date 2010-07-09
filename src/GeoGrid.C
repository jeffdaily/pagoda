#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

using std::back_inserter;
using std::copy;
using std::istream_iterator;
using std::istringstream;
using std::string;
using std::vector;

#include "Attribute.H"
#include "Dataset.H"
#include "Dimension.H"
#include "GeoGrid.H"
#include "StringComparator.H"
#include "Variable.H"


static inline vector<string> split(const string &s)
{
    vector<string> tokens;
    istringstream iss(s);
    copy(istream_iterator<string>(iss),
            istream_iterator<string>(),
            back_inserter<vector<string> >(tokens));
    return tokens;
}


/**
 * Locate one or more geodesic grids.
 *
 * @param[in] dataset where to look
 */
vector<Grid*> GeoGrid::get_grids(const Dataset *dataset)
{
    vector<Grid*> results;
    int count = 0;
    vector<Variable*> vars = dataset->get_vars();
    vector<Variable*>::const_iterator var_it = vars.begin();
    vector<Variable*>::const_iterator var_end = vars.end();
    vector<Attribute*> atts;
    vector<Attribute*>::iterator att_it;
    vector<Attribute*>::iterator att_end;

    // first, look for one or more special "grid" variable(s)
    // it is dimensionless with a cell_type attribute of "hex"
    for ( ; var_it!=var_end; ++var_it) {
        Variable *var = *var_it;
        if (var->get_ndim() == 0) {
            atts = var->get_atts();
            att_it = atts.begin();
            att_end = atts.end();
            for ( ; att_it!=att_end; ++att_it) {
                Attribute *att = *att_it;
                if (att->get_name() == "cell_type") {
                    if (att->get_string() == "hex") {
                        results.push_back(new GeoGrid(dataset, var));
                        break;
                    }
                }
            }
        }
    }

    // all else failed, look for well known variables/attributes
    // also at this point assume a single Grid in the Dataset

    // look for "grid" global attribute with value "geodesic"
    atts = dataset->get_atts();
    att_it = atts.begin();
    att_end = atts.end();
    for ( ; att_it!=att_end; ++att_it) {
        Attribute *att = *att_it;
        if (att->get_name() == "grid") {
            if (att->get_string() == "geodesic") {
                results.push_back(new GeoGrid(dataset));
                break;
            }
        }
    }

    return results;
}


GeoGrid::GeoGrid()
    :   dataset(NULL)
{
    // TODO throw exception
}


GeoGrid::GeoGrid(const GeoGrid &that)
    :   dataset(that.dataset)
{
    // TODO throw exception
}


/**
 * Constructor given a "grid" Variable.
 */
GeoGrid::GeoGrid(const Dataset *dataset, const Variable *grid_var)
    :   dataset(dataset)
    ,   grid_var(grid_var)
{
}


GeoGrid::~GeoGrid()
{
    dataset = NULL;
}


GridType GeoGrid::get_type() const
{
    return GridType::GEODESIC;
}


Variable* GeoGrid::get_coord(const string &att_name, const string &coord_name)
{
    if (grid_var) {
        Attribute *att = grid_var->get_att(att_name);
        if (att) {
            vector<string> parts = split(att->get_string());
            if (parts.size() != 2) {
                throw GridException("expected " + att_name + " attribute "
                        "to have two values", parts.size());
            } else {
                for (size_t i=0; i<parts.size(); ++i) {
                    Variable *var = dataset->get_var(parts[i]);
                    StringComparator cmp(coord_name, true, true);
                    if (!var) {
                        throw GridException(
                                "could not locate variable" + parts[i]);
                    }
                    if (cmp(var->get_standard_name())
                            || cmp(var->get_long_name())) {
                        return var;
                    }
                }
            }
        }
    }

    return NULL;
}


Variable* GeoGrid::get_cell_lat()
{
    return get_coord("coordinates_cells", "latitude");
}


Variable* GeoGrid::get_cell_lon()
{
    return get_coord("coordinates_cells", "longitude");
}


Variable* GeoGrid::get_edge_lat()
{
    return get_coord("coordinates_edges", "latitude");
}


Variable* GeoGrid::get_edge_lon()
{
    return get_coord("coordinates_edges", "longitude");
}


Variable* GeoGrid::get_corner_lat()
{
    return get_coord("coordinates_corners", "latitude");
}


Variable* GeoGrid::get_corner_lon()
{
    return get_coord("coordinates_corners", "longitude");
}


Dimension* GeoGrid::get_dim(const string &att_name)
{
    if (grid_var) {
        Attribute *att = grid_var->get_att(att_name);
        if (att) {
            vector<string> parts = split(att->get_string());
            if (parts.size() != 2) {
                throw GridException("expected " + att_name + " attribute "
                        "to have two values", parts.size());
            } else {
                for (size_t i=0; i<parts.size(); ++i) {
                    Variable *var = dataset->get_var(parts[i]);
                    if (!var) {
                        throw GridException(
                                "could not locate variable" + parts[i]);
                    }
                    return var->get_dims().at(0);
                }
            }
        }
    }

    return NULL;
}


Dimension* GeoGrid::get_cell_dim()
{
    return get_dim("coordinates_cells");
}


Dimension* GeoGrid::get_edge_dim()
{
    return get_dim("coordinates_edges");
}


Dimension* GeoGrid::get_corner_dim()
{
    return get_dim("coordinates_corners");
}


Variable* GeoGrid::get_topology(const string &att_name)
{
    if (grid_var) {
        Attribute *att = grid_var->get_att(att_name);
        if (att) {
            string var_name = att->get_string();
            Variable *var = dataset->get_var(var_name);
            if (!var) {
                throw GridException("could not locate variable " + var_name);
            }
            return var;
        }
    }

    return NULL;
}


Variable* GeoGrid::get_cell_cells()
{
    return get_topology("cell_cells");
}


Variable* GeoGrid::get_cell_edges()
{
    return get_topology("cell_edges");
}


Variable* GeoGrid::get_cell_corners()
{
    return get_topology("cell_corners");
}


Variable* GeoGrid::get_edge_cells()
{
    return NULL;
}


Variable* GeoGrid::get_edge_edges()
{
    return NULL;
}


Variable* GeoGrid::get_edge_corners()
{
    return get_topology("edge_corners");
}


Variable* GeoGrid::get_corner_cells()
{
    return NULL;
}


Variable* GeoGrid::get_corner_edges()
{
    return NULL;
}


Variable* GeoGrid::get_corner_corners()
{
    return NULL;
}


const Dataset* GeoGrid::get_dataset() const
{
    return dataset;
}
