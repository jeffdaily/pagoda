#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "Attribute.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "Error.H"
#include "GeoGrid.H"
#include "StringComparator.H"
#include "Util.H"
#include "Variable.H"

using std::back_inserter;
using std::copy;
using std::istream_iterator;
using std::istringstream;
using std::string;
using std::vector;


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
    Variable *grid_center_lat;
    Variable *grid_center_lon;

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

    // look for "grid_center_lat" and "grid_center_lon" Variables which share
    // the same Dimension
    grid_center_lat = dataset->get_var("grid_center_lat");
    grid_center_lon = dataset->get_var("grid_center_lon");
    if (grid_center_lat && grid_center_lon) {
        if (grid_center_lat->get_dims() == grid_center_lon->get_dims()) {
            results.push_back(new GeoGrid(dataset));
        }
    }

    return results;
}


GeoGrid::GeoGrid()
    :   dataset(NULL)
{
    ERR("GeoGrid::GeoGrid() ctor not supported");
}


GeoGrid::GeoGrid(const GeoGrid &that)
    :   dataset(that.dataset)
{
    ERR("GeoGrid::GeoGrid() copy ctor not supported");
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


//#include "Debug.H"
Variable* GeoGrid::get_coord(const string &att_name,
        const string &coord_name, const string &dim_name)
{
    TRACER("GeoGrid::get_coord(%s,%s,%s)\n", att_name.c_str(),
            coord_name.c_str(), dim_name.c_str());
    if (grid_var) {
        Attribute *att = grid_var->get_att(att_name);
        if (att) {
            vector<string> parts = pagoda::split(att->get_string());
            vector<string>::iterator part;
            if (parts.size() != 2) {
                throw GridException("expected " + att_name + " attribute "
                        "to have two values", parts.size());
            }
            for (part=parts.begin(); part!=parts.end(); ++part) {
                Variable *var = dataset->get_var(*part);
                StringComparator cmp("", true, true);
                string standard_name;
                string long_name;
                if (!var) {
                    throw GridException("could not locate variable " + *part);
                }
                standard_name = var->get_standard_name();
                long_name = var->get_long_name();
                TRACER("\tcomparing '%s' '%s' '%s'\n",
                        coord_name.c_str(), standard_name.c_str(),
                        long_name.c_str());
                if (!standard_name.empty()) {
                    cmp.set_value(standard_name);
                    if (cmp(coord_name)) {
                        TRACER("\tfound\n");
                        return var;
                    }
                } else if (!long_name.empty()) {
                    cmp.set_value(long_name);
                    if (cmp(coord_name)) {
                        TRACER("\tfound\n");
                        return var;
                    }
                } else {
                    TRACER("\tNOT FOUND\n");
                }
            }
        }
    } else {
        // look for a variable with the given dimension name as its only
        // dimension and then for a substring match within the "standard_name"
        // or "long_name" attributes
        vector<Variable*> vars = get_dataset()->get_vars();
        vector<Variable*>::iterator var_it;
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            Variable *var = *var_it;
            vector<Dimension*> dims = var->get_dims();
            if (dims.size() == 1) {
                /*
                pagoda::print_zero("found a dim.size() == 1 %s\n",
                        dims[0]->get_name().c_str());
                */
                if (dims[0]->get_name() == dim_name) {
                    StringComparator cmp(coord_name, true, true);
                    if (cmp(var->get_standard_name())
                            || cmp(var->get_long_name())) {
                        return var;
                        /*
                    } else {
                        pagoda::print_zero(
                                "cmp did not match %s to either %s or %s\n",
                                coord_name.c_str(),
                                var->get_standard_name().c_str(),
                                var->get_long_name().c_str());
                        */
                    }
                }
            }
        }
    }

    return NULL;
}


Variable* GeoGrid::get_cell_lat()
{
    return get_coord("coordinates_cells", "latitude", "cells");
}


Variable* GeoGrid::get_cell_lon()
{
    return get_coord("coordinates_cells", "longitude", "cells");
}


Variable* GeoGrid::get_edge_lat()
{
    return get_coord("coordinates_edges", "latitude", "edges");
}


Variable* GeoGrid::get_edge_lon()
{
    return get_coord("coordinates_edges", "longitude", "edges");
}


Variable* GeoGrid::get_corner_lat()
{
    return get_coord("coordinates_corners", "latitude", "corners");
}


Variable* GeoGrid::get_corner_lon()
{
    return get_coord("coordinates_corners", "longitude", "corners");
}


bool GeoGrid::is_radians()
{
    vector<Variable* (GeoGrid::*)()> funcs;
    funcs.push_back(&GeoGrid::get_cell_lat);
    funcs.push_back(&GeoGrid::get_cell_lon);
    funcs.push_back(&GeoGrid::get_edge_lat);
    funcs.push_back(&GeoGrid::get_edge_lon);
    funcs.push_back(&GeoGrid::get_corner_lat);
    funcs.push_back(&GeoGrid::get_corner_lon);

    for (size_t i=0; i<funcs.size(); ++i) {
        Variable *var;
        Attribute *att;
        Variable* (GeoGrid::*func)() = funcs[i];
        if ((var = (this->*func)())) {
            if ((att = var->get_att("units"))
                    && att->get_string() == "radians") {
                return true;
            }
        }
    }

    return false;
}


Dimension* GeoGrid::get_dim(const string &att_name, const string &dim_name)
{
    if (grid_var) {
        Attribute *att = grid_var->get_att(att_name);
        if (att) {
            vector<string> parts = pagoda::split(att->get_string());
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

    // if all else fails, use hard-coded name...
    return dataset->get_dim(dim_name);
}


Dimension* GeoGrid::get_cell_dim()
{
    return get_dim("coordinates_cells", "cells");
}


Dimension* GeoGrid::get_edge_dim()
{
    return get_dim("coordinates_edges", "edges");
}


Dimension* GeoGrid::get_corner_dim()
{
    return get_dim("coordinates_corners", "corners");
}


Variable* GeoGrid::get_topology(const string &att_name, const string &var_name)
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

    // if all else fails, try given name
    return dataset->get_var(var_name);
}


Variable* GeoGrid::get_cell_cells()
{
    return get_topology("cell_cells", "cell_neighbors");
}


Variable* GeoGrid::get_cell_edges()
{
    return get_topology("cell_edges", "cell_edges");
}


Variable* GeoGrid::get_cell_corners()
{
    return get_topology("cell_corners", "cell_corners");
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
    Variable *var = get_topology("edge_corners", "edgecorners");
    if (!var) {
        var = get_topology("edge_corners", "edge_corners");
    }
    return var;
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
