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
#include "Dimension.H"
#include "Error.H"
#include "Print.H"
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
    for (; var_it!=var_end; ++var_it) {
        Variable *var = *var_it;
        if (var->get_ndim() == 0) {
            atts = var->get_atts();
            att_it = atts.begin();
            att_end = atts.end();
            for (; att_it!=att_end; ++att_it) {
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
    if (!results.empty()) {
        return results;
    }

    // first look for lat and lon coordinate variables with appropriate units
    // then look for Y and X axis attributes
    // then look for "latitude" and "longitude" standard_name attributes
    found_lat = false;
    found_lon = false;
    var_it = vars.begin();
    for (; var_it!=var_end; ++var_it) {
        Variable *var = *var_it;
        
        if (var->get_ndim() == 1
                && var->get_name() == var->get_dims()[0]->get_name()) {
            Attribute *att = NULL;
            if ((att = var->get_att("units"))) {
                if (LATITUDE_UNITS.count(att->get_string())) {
                    //pagoda::println_zero("units latitude");
                    found_lat = true;
                }
                else if (LONGITUDE_UNITS.count(att->get_string())) {
                    //pagoda::println_zero("units longitude");
                    found_lon = true;
                }
            }
            if ((att = var->get_att("units"))) {
                if (att->get_string() == "Y") {
                    //pagoda::println_zero("axis latitude");
                    found_lat = true;
                }
                else if (att->get_string() == "X") {
                    //pagoda::println_zero("axis longitude");
                    found_lon = true;
                }
            }
            if ((att = var->get_att("standard_name"))) {
                if (att->get_string() == "latitude") {
                    //pagoda::println_zero("standard_name latitude");
                    found_lat = true;
                }
                else if (att->get_string() == "longitude") {
                    //pagoda::println_zero("standard_name longitude");
                    found_lon = true;
                }
            }
        }
    }
    if (found_lat && found_lon) {
        results.push_back(new RegularGrid(dataset));
    }

    return results;
}


RegularGrid::RegularGrid()
    :   Grid()
    ,   dataset(NULL)
    ,   grid_var(NULL)
{
    ERR("RegularGrid::RegularGrid() ctor not supported");
}


RegularGrid::RegularGrid(const RegularGrid &that)
    :   Grid()
    ,   dataset(that.dataset)
    ,   grid_var(NULL)
{
    ERR("RegularGrid::RegularGrid() copy ctor not supported");
}


/**
 * Constructor given a "grid" Variable.
 */
RegularGrid::RegularGrid(const Dataset *dataset, const Variable *grid_var)
    :   Grid()
    ,   dataset(dataset)
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


Variable* RegularGrid::get_cell_lat()
{
    return NULL;
}


Variable* RegularGrid::get_cell_lon()
{
    return NULL;
}


Variable* RegularGrid::get_edge_lat()
{
    return NULL;
}


Variable* RegularGrid::get_edge_lon()
{
    return NULL;
}


Variable* RegularGrid::get_corner_lat()
{
    return NULL;
}


Variable* RegularGrid::get_corner_lon()
{
    return NULL;
}


bool RegularGrid::is_radians()
{
    vector<Variable*(RegularGrid::*)()> funcs;
    funcs.push_back(&RegularGrid::get_cell_lat);
    funcs.push_back(&RegularGrid::get_cell_lon);
    funcs.push_back(&RegularGrid::get_edge_lat);
    funcs.push_back(&RegularGrid::get_edge_lon);
    funcs.push_back(&RegularGrid::get_corner_lat);
    funcs.push_back(&RegularGrid::get_corner_lon);
    funcs.push_back(&RegularGrid::get_lat);
    funcs.push_back(&RegularGrid::get_lon);

    for (size_t i=0; i<funcs.size(); ++i) {
        Variable *var;
        Attribute *att;
        Variable*(RegularGrid::*func)() = funcs[i];
        if ((var = (this->*func)())) {
            if ((att = var->get_att("units"))
                    && att->get_string() == "radians") {
                return true;
            }
        }
    }

    return false;
}


Dimension* RegularGrid::get_cell_dim()
{
    return NULL;
}


Dimension* RegularGrid::get_edge_dim()
{
    return NULL;
}


Dimension* RegularGrid::get_corner_dim()
{
    return NULL;
}


Variable* RegularGrid::get_cell_cells()
{
    return NULL;
}


Variable* RegularGrid::get_cell_edges()
{
    return NULL;
}


Variable* RegularGrid::get_cell_corners()
{
    return NULL;
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
    return NULL;
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


static Variable* get_lat_or_lon(const Dataset *dataset, bool get_lat)
{
    vector<Variable*> vars = dataset->get_vars();
    vector<Variable*>::const_iterator var_it = vars.begin();
    vector<Variable*>::const_iterator var_end = vars.end();

    var_it = vars.begin();
    for (; var_it!=var_end; ++var_it) {
        Variable *var = *var_it;
        
        if (var->get_ndim() == 1
                && var->get_name() == var->get_dims()[0]->get_name()) {
            Attribute *att = NULL;
            if ((att = var->get_att("units"))) {
                if (get_lat) {
                    if (LATITUDE_UNITS.count(att->get_string())) {
                        //pagoda::println_zero("units lat " + var->get_name());
                        return var;
                    }
                } else {
                    if (LONGITUDE_UNITS.count(att->get_string())) {
                        //pagoda::println_zero("units lon " + var->get_name());
                        return var;
                    }
                }
            }
            if ((att = var->get_att("units"))) {
                if (get_lat) {
                    if (att->get_string() == "Y") {
                        //pagoda::println_zero("axis lat " + var->get_name());
                        return var;
                    }
                } else {
                    if (att->get_string() == "X") {
                        //pagoda::println_zero("axis lon " + var->get_name());
                        return var;
                    }
                }
            }
            if ((att = var->get_att("standard_name"))) {
                if (get_lat) {
                    if (att->get_string() == "latitude") {
                        //pagoda::println_zero("s_n lat " + var->get_name());
                        return var;
                    }
                } else {
                    if (att->get_string() == "longitude") {
                        //pagoda::println_zero("s_n lon " + var->get_name());
                        return var;
                    }
                }
            }
        }
    }

    return NULL;
}


Variable* RegularGrid::get_lat()
{
    return get_lat_or_lon(dataset, true);
}


Variable* RegularGrid::get_lon()
{
    return get_lat_or_lon(dataset, false);
}


Dimension* RegularGrid::get_lat_dim()
{
    Variable *var;

    if ((var = get_lat())) {
        return var->get_dims().at(0);
    }

    return NULL;
}


Dimension* RegularGrid::get_lon_dim()
{
    Variable *var;

    if ((var = get_lon())) {
        return var->get_dims().at(0);
    }

    return NULL;
}

