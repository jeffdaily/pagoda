#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <cctype>
#include <iostream>
#include <set>

#include "Attribute.H"
#include "BoundaryVariable.H"
#include "ConnectivityVariable.H"
#include "CoordinateVariable.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "Error.H"
#include "LatLonBox.H"
#include "Mask.H"
#include "NetcdfDataset.H"
#include "NetcdfVariable.H"
#include "Pnetcdf.H"
#include "Slice.H"
#include "StringComparator.H"
#include "Timing.H"
#include "Util.H"
#include "Variable.H"

using std::cerr;
using std::endl;
using std::fill;
using std::find;
using std::set;
using std::tolower;
using std::transform;


Dataset* Dataset::open(const string &filename)
{
    //TIMING("Dataset::open(string)");
    Dataset *dataset = NULL;
    string EXT_NC(".nc");
    if (Util::ends_with(filename, EXT_NC)) {
        dataset = new NetcdfDataset(filename);
    }
    if (dataset) {
        //dataset->decorate();
    }
    return dataset;
}


Dataset::Dataset()
{
    TIMING("Dataset::Dataset()");
}


Dataset::~Dataset()
{
    TIMING("Dataset::~Dataset()");
}


Attribute* Dataset::find_att(const string &name, bool ignore_case, bool within)
{
    TIMING("Dataset::find_att(string,bool,bool)");
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::const_iterator it = atts.begin();
    vector<Attribute*>::const_iterator end = atts.end();
    StringComparator cmp;

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(name)) {
            return *it;
        }
    }

    return NULL;
}


Attribute* Dataset::find_att(const vector<string> &names, bool ignore_case,
        bool within)
{
    TIMING("Dataset::find_att(vector<string>,bool,bool)");
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::const_iterator it = atts.begin();
    vector<Attribute*>::const_iterator end = atts.end();
    StringComparator cmp;

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(names)) {
            return *it;
        }
    }

    return NULL;
}


Dimension* Dataset::find_dim(const string &name, bool ignore_case, bool within)
{
    TIMING("Dataset::find_dim(string,bool,bool)");
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::const_iterator it = dims.begin();
    vector<Dimension*>::const_iterator end = dims.end();
    StringComparator cmp;

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(name)) {
            return *it;
        }
    }

    return NULL;
}


Variable* Dataset::find_var(const string &name, bool ignore_case, bool within)
{
    TIMING("Dataset::find_var(string,bool,bool)");
    vector<Variable*> vars = get_vars();
    vector<Variable*>::const_iterator it = vars.begin();
    vector<Variable*>::const_iterator end = vars.end();
    StringComparator cmp;

    cmp.set_ignore_case(ignore_case);
    cmp.set_within(within);
    for (; it!=end; ++it) {
        cmp.set_value((*it)->get_name());
        if (cmp(name)) {
            return *it;
        }
    }

    return NULL;
}


ostream& operator << (ostream &os, const Dataset *dataset)
{
    return dataset->print(os);
}


/**
 * Examine attributes and enhance/wrap Variables appropriately.
 */
void Dataset::decorate()
{
    TIMING("Dataset::decorate()");
    static vector<string> lat_units;
    if (lat_units.empty()) {
        lat_units.push_back(string("degrees_north"));
        lat_units.push_back(string("degree_north"));
        lat_units.push_back(string("degree_N"));
        lat_units.push_back(string("degrees_N"));
        lat_units.push_back(string("degreeN"));
        lat_units.push_back(string("degreesN"));
    }
    static vector<string> lon_units;
    if (lon_units.empty()) {
        lon_units.push_back(string("degrees_east"));
        lon_units.push_back(string("degree_east"));
        lon_units.push_back(string("degree_E"));
        lon_units.push_back(string("degrees_E"));
        lon_units.push_back(string("degreeE"));
        lon_units.push_back(string("degreesE"));
    }
    static vector<string> vert_units;
    if (vert_units.empty()) {
        vert_units.push_back(string("bar"));
        vert_units.push_back(string("millibar"));
        vert_units.push_back(string("decibar"));
        vert_units.push_back(string("atm")); // atmosphere
        vert_units.push_back(string("Pa")); // pascal
        vert_units.push_back(string("hPa")); // hectopascal
        vert_units.push_back(string("meter"));
        vert_units.push_back(string("metre"));
        vert_units.push_back(string("m")); // meter
        vert_units.push_back(string("kilometer"));
        vert_units.push_back(string("km")); // kilometer
        // deprecated
        vert_units.push_back(string("level"));
        vert_units.push_back(string("layer"));
        vert_units.push_back(string("interface"));
        vert_units.push_back(string("sigma_level"));
    }

    vector<Variable*> vars = get_vars();
    vector<Variable*>::iterator var_it;

    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        //cerr << "Attempting to decorate " << var->get_name() << endl;
        Attribute *att;
        if ((att = var->find_att(string("bounds")))) {
            string val = att->get_string();
            Variable *bound_var;
            if ((bound_var = find_var(val))) {
                vector<Variable*>::iterator it;
                it = find(vars.begin(), vars.end(), bound_var);
                (*it) = new BoundaryVariable(bound_var, var);
            }
        }
        if ((att = var->find_att(string("units")))) {
            string val = att->get_string();
            if (find(lat_units.begin(),lat_units.end(),val) != lat_units.end())
            {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
            if (find(lon_units.begin(),lon_units.end(),val) != lon_units.end())
            {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
            if (find(vert_units.begin(),vert_units.end(),val)
                    != vert_units.end()) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
        }
        if ((att = var->find_att(string("standard_name")))) {
            string val = att->get_string();
            if ("latitude" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            } else if ("longitude" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
        }
        if ((att = var->find_att(string("axis")))) {
            string val = att->get_string();
            if ("Y" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            } else if ("X" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            } else if ("Z" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
        }
        if ((att = var->find_att(string("positive")))) {
            string val = att->get_string();
            transform(val.begin(),val.end(),val.begin(),(int(*)(int))tolower);
            if ("up" == val || "down" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
        }
        if (var->get_name() == "cell_neighbors") {
            Dimension *dim = find_dim(string("cells"));
            if (dim) {
                (*var_it) = new ConnectivityVariable(var, dim);
            }
        }
        if (var->get_name() == "cell_corners") {
            Dimension *dim = find_dim(string("corners"));
            if (dim) {
                (*var_it) = new ConnectivityVariable(var, dim);
            }
        }
        if (var->get_name() == "cell_edges") {
            Dimension *dim = find_dim(string("edges"));
            if (dim) {
                (*var_it) = new ConnectivityVariable(var, dim);
            }
        }
    }
#ifdef DEBUG
    if (0 == GA_Nodeid()) {
        cerr << "Variables after decoration" << endl;
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            cerr << (*var_it) << endl;
        }
    }
    GA_Sync();
#endif // DEBUG
}
