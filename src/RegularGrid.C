#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <iostream>
#include <iterator>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "Attribute.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "Error.H"
#include "RegularGrid.H"
#include "StringComparator.H"
#include "Util.H"
#include "Variable.H"

using std::back_inserter;
using std::copy;
using std::istream_iterator;
using std::istringstream;
using std::set;
using std::string;
using std::vector;

static set<string> LATITUDE_UNITS;
static set<string> LONGITUDE_UNITS;


/**
 * Locate one or more geodesic grids.
 *
 * @param[in] dataset where to look
 */
vector<Grid*> RegularGrid::get_grids(const Dataset *dataset)
{
    vector<Grid*> results;
    vector<Variable*> vars = dataset->get_vars();
    vector<Variable*>::const_iterator var_it = vars.begin();
    vector<Variable*>::const_iterator var_end = vars.end();
    vector<Attribute*> atts;
    vector<Attribute*>::iterator att_it;
    vector<Attribute*>::iterator att_end;
    bool found_lat = false;
    bool found_lon = false;

    //pagoda::println_zero("RegularGrid::get_grids");

    // initialize unit sets first time only
    if (LATITUDE_UNITS.empty()) {
        LATITUDE_UNITS.insert("degrees_north");
        LATITUDE_UNITS.insert("degree_north");
        LATITUDE_UNITS.insert("degrees_N");
        LATITUDE_UNITS.insert("degree_N");
        LATITUDE_UNITS.insert("degreesN");
        LATITUDE_UNITS.insert("degreeN");
    }
    if (LONGITUDE_UNITS.empty()) {
        LONGITUDE_UNITS.insert("degrees_east");
        LONGITUDE_UNITS.insert("degree_east");
        LONGITUDE_UNITS.insert("degrees_E");
        LONGITUDE_UNITS.insert("degree_E");
        LONGITUDE_UNITS.insert("degreesE");
        LONGITUDE_UNITS.insert("degreeE");
    }

    // first, look for one or more special "grid" variable(s)
    // it is dimensionless with a cell_type attribute of "rect"
    for ( ; var_it!=var_end; ++var_it) {
        Variable *var = *var_it;
        if (var->get_ndim() == 0) {
            atts = var->get_atts();
            att_it = atts.begin();
            att_end = atts.end();
            for ( ; att_it!=att_end; ++att_it) {
                Attribute *att = *att_it;
                if (att->get_name() == "cell_type") {
                    if (att->get_string() == "rect") {
                        results.push_back(new RegularGrid(dataset, var));
                        break;
                    }
                }
            }
        }
    }

    // all else failed, look for 1-dimensional lat and lon coordinate
    // variables
    // also at this point assume a single Grid in the Dataset
    var_it = vars.begin();
    for ( ; var_it!=var_end; ++var_it) {
        Variable *var = *var_it;
        //pagoda::println_zero("looking at %s", var->get_name().c_str());
        if (var->get_ndim() == 1
                && var->get_dims()[0]->get_name() == var->get_name()) {
            //pagoda::println_zero("\tcoordinate var");
            atts = var->get_atts();
            att_end = atts.end();
            // look for latitude
            att_it = atts.begin();
            for ( ; att_it!=att_end; ++att_it) {
                Attribute *att = *att_it;
                if (att->get_name() == "standard_name") {
                    if (att->get_string() == "latitude") {
                        found_lat = true;
                        //pagoda::println_zero("\tstandard_name latitude");
                        break;
                    }
                } else if (att->get_name() == "units") {
                    if (LATITUDE_UNITS.count(att->get_string())) {
                        found_lat = true;
                        //pagoda::println_zero("\tunits latitude");
                        break;
                    }
                }
            }
            // look for longitude
            att_it = atts.begin();
            for ( ; att_it!=att_end; ++att_it) {
                Attribute *att = *att_it;
                if (att->get_name() == "standard_name") {
                    if (att->get_string() == "longitude") {
                        found_lon = true;
                        //pagoda::println_zero("\tstandard_name longitude");
                        break;
                    }
                } else if (att->get_name() == "units") {
                    if (LONGITUDE_UNITS.count(att->get_string())) {
                        found_lon = true;
                        //pagoda::println_zero("\tunits longitude");
                        break;
                    }
                }
            }
        }
        if (found_lat && found_lon) {
            results.push_back(new RegularGrid(dataset));
            break;
        }
    }

    return results;
}


RegularGrid::RegularGrid()
    :   dataset(NULL)
{
    ERR("RegularGrid::RegularGrid() ctor not supported");
}


RegularGrid::RegularGrid(const RegularGrid &that)
    :   dataset(that.dataset)
{
    ERR("RegularGrid::RegularGrid() copy ctor not supported");
}


/**
 * Constructor given a "grid" Variable.
 */
RegularGrid::RegularGrid(const Dataset *dataset, const Variable *grid_var)
    :   dataset(dataset)
    ,   grid_var(grid_var)
{
}


RegularGrid::~RegularGrid()
{
    dataset = NULL;
}


GridType RegularGrid::get_type() const
{
    return GridType::REGULAR;
}


Variable* RegularGrid::get_coord(const string &att_name,
        const string &coord_name, const string &dim_name)
{
    TRACER("RegularGrid::get_coord(%s,%s,%s)\n", att_name.c_str(),
            coord_name.c_str(), dim_name.c_str());
    if (grid_var) {
        Attribute *att = grid_var->get_att(att_name);
        if (att) {
            vector<string> parts = pagoda::split(att->get_string());
            vector<string>::iterator part;
            if (parts.size() != 2) {
                EXCEPT(GridException,
                        "expected " + att_name + " attribute "
                        "to have two values", parts.size());
            }
            for (part=parts.begin(); part!=parts.end(); ++part) {
                Variable *var = dataset->get_var(*part);
                StringComparator cmp("", true, true);
                string standard_name;
                string long_name;
                if (!var) {
                    EXCEPT(GridException,
                            "could not locate variable " + *part, 0);
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


Variable* RegularGrid::get_cell_lat()
{
    return get_coord("coordinates_cells", "latitude", "cells");
}


Variable* RegularGrid::get_cell_lon()
{
    return get_coord("coordinates_cells", "longitude", "cells");
}


Variable* RegularGrid::get_edge_lat()
{
    return get_coord("coordinates_edges", "latitude", "edges");
}


Variable* RegularGrid::get_edge_lon()
{
    return get_coord("coordinates_edges", "longitude", "edges");
}


Variable* RegularGrid::get_corner_lat()
{
    return get_coord("coordinates_corners", "latitude", "corners");
}


Variable* RegularGrid::get_corner_lon()
{
    return get_coord("coordinates_corners", "longitude", "corners");
}


bool RegularGrid::is_radians()
{
    vector<Variable* (RegularGrid::*)()> funcs;
    funcs.push_back(&RegularGrid::get_cell_lat);
    funcs.push_back(&RegularGrid::get_cell_lon);
    funcs.push_back(&RegularGrid::get_edge_lat);
    funcs.push_back(&RegularGrid::get_edge_lon);
    funcs.push_back(&RegularGrid::get_corner_lat);
    funcs.push_back(&RegularGrid::get_corner_lon);

    for (size_t i=0; i<funcs.size(); ++i) {
        Variable *var;
        Attribute *att;
        Variable* (RegularGrid::*func)() = funcs[i];
        if ((var = (this->*func)())) {
            if ((att = var->get_att("units"))
                    && att->get_string() == "radians") {
                return true;
            }
        }
    }

    return false;
}


Dimension* RegularGrid::get_dim(const string &att_name, const string &dim_name)
{
    if (grid_var) {
        Attribute *att = grid_var->get_att(att_name);
        if (att) {
            vector<string> parts = pagoda::split(att->get_string());
            if (parts.size() != 2) {
                EXCEPT(GridException,
                        "expected " + att_name + " attribute "
                        "to have two values", parts.size());
            } else {
                for (size_t i=0; i<parts.size(); ++i) {
                    Variable *var = dataset->get_var(parts[i]);
                    if (!var) {
                        EXCEPT(GridException,
                                "could not locate variable" + parts[i], 0);
                    }
                    return var->get_dims().at(0);
                }
            }
        }
    }

    // if all else fails, use hard-coded name...
    return dataset->get_dim(dim_name);
}


Dimension* RegularGrid::get_cell_dim()
{
    return get_dim("coordinates_cells", "cells");
}


Dimension* RegularGrid::get_edge_dim()
{
    return get_dim("coordinates_edges", "edges");
}


Dimension* RegularGrid::get_corner_dim()
{
    return get_dim("coordinates_corners", "corners");
}


Variable* RegularGrid::get_topology(const string &att_name, const string &var_name)
{
    if (grid_var) {
        Attribute *att = grid_var->get_att(att_name);
        if (att) {
            string var_name = att->get_string();
            Variable *var = dataset->get_var(var_name);
            if (!var) {
                EXCEPT(GridException,
                        "could not locate variable " + var_name, 0);
            }
            return var;
        }
    }

    // if all else fails, try given name
    return dataset->get_var(var_name);
}


Variable* RegularGrid::get_cell_cells()
{
    return get_topology("cell_cells", "cell_neighbors");
}


Variable* RegularGrid::get_cell_edges()
{
    return get_topology("cell_edges", "cell_edges");
}


Variable* RegularGrid::get_cell_corners()
{
    return get_topology("cell_corners", "cell_corners");
}


Variable* RegularGrid::get_edge_cells()
{
    return NULL;
}


Variable* RegularGrid::get_edge_edges()
{
    return NULL;
}


Variable* RegularGrid::get_edge_corners()
{
    Variable *var = get_topology("edge_corners", "edgecorners");
    if (!var) {
        var = get_topology("edge_corners", "edge_corners");
    }
    return var;
}


Variable* RegularGrid::get_corner_cells()
{
    return NULL;
}


Variable* RegularGrid::get_corner_edges()
{
    return NULL;
}


Variable* RegularGrid::get_corner_corners()
{
    return NULL;
}


const Dataset* RegularGrid::get_dataset() const
{
    return dataset;
}
