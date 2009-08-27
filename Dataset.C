#include <algorithm>
    using std::find;
#include <cctype>
    using std::tolower;
#include <iostream>
    using std::cerr;
    using std::endl;

#include "Attribute.H"
#include "BoundaryVariable.H"
#include "ConnectivityVariable.H"
#include "CoordinateVariable.H"
#include "Dataset.H"
#include "Dimension.H"
#include "DistributedMask.H"
#include "LatLonBox.H"
#include "LocalMask.H"
#include "NetcdfDataset.H"
#include "Mask.H"
#include "Slice.H"
#include "Util.H"
#include "Variable.H"


Dataset* Dataset::open(const string &filename)
{
    Dataset *dataset = NULL;
    if (Util::ends_with(filename, ".nc")) {
        dataset = new NetcdfDataset(filename);
    }
    if (dataset) {
        dataset->decorate();
        //dataset->create_masks();
    }
    return dataset;
}


Dataset::Dataset()
{
}


Dataset::~Dataset()
{
}


Attribute* Dataset::find_att(const string &name, bool ignore_case)
{
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::iterator result;
    result = Util::find(atts, name, ignore_case);
    if (result != atts.end())
        return *result;
    return NULL;
}


Attribute* Dataset::find_att(const vector<string> &names, bool ignore_case)
{
    vector<Attribute*> atts = get_atts();
    vector<Attribute*>::iterator result;
    result = Util::find(atts, names, ignore_case);
    if (result != atts.end())
        return *result;
    return NULL;
}


Dimension* Dataset::find_dim(const string &name, bool ignore_case)
{
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::iterator result;
    result = Util::find(dims, name, ignore_case);
    if (result != dims.end())
        return *result;
    return NULL;
}


Variable* Dataset::find_var(const string &name, bool ignore_case)
{
    vector<Variable*> vars = get_vars();
    vector<Variable*>::iterator result;
    result = Util::find(vars, name, ignore_case);
    if (result != vars.end())
        return *result;
    return NULL;
}


void Dataset::create_masks()
{
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::iterator dim_it;
    for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
        (*dim_it)->set_mask(new DistributedMask(*dim_it, 1));
    }
}


void Dataset::adjust_masks(const vector<DimSlice> &slices)
{
    vector<Dimension*> dims = get_dims();
    vector<Dimension*>::iterator dim_it;
    vector<DimSlice>::const_iterator slice_it;

    // we're iterating over the command-line specified slices to create masks
    for (slice_it=slices.begin(); slice_it!=slices.end(); ++slice_it) {
        //Mask *mask;

        // find the Dimension
        for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
            if ((*dim_it)->get_name() == slice_it->get_name()) break;
        }
        if (dim_it == dims.end()) {
            cerr << "Sliced dimension '" << slice_it->get_name()
                << "' does not exist" << endl;
            continue;
        }

        // adjust the Mask based on the current Slice
        (*dim_it)->get_mask()->adjust(*slice_it);
    }
}


/**
 * Special-case mask creation using a Lat/Lon combination.
 *
 * Different than creating masks solely based on a single Slice at a time,
 * this special case allows the lat and lon dimensions to be subset
 * simultaenously. This is most important when dealing with a cell-based
 * grid where the lat and lon coordinate variables share the same dimension.
 * In that case, if we were to generate masks one at a time, we'd get, in
 * logic terms, an OR over the shared dimension mask. What we really want,
 * whether the grid is cell- or rectangular-based, is an AND result -- a
 * lat lon box instead of a lat lon cross.
 */
void Dataset::adjust_masks(const LatLonBox &box)
{
    if (box != LatLonBox::GLOBAL) {
        // TODO how to locate the lat/lon dims?
        // TODO subset anything with lat or lon dimensions such as
        // corner/edge variables
        // Likely solution is to iterate over all Variables and examine them
        // for special attributes
        Variable *lat = find_var("grid_center_lat");
        Variable *lon = find_var("grid_center_lon");
        lon->get_dims()[0]->get_mask()->adjust(box, lat, lon);
    }
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
    static vector<string> lat_units;
    if (lat_units.empty()) {
        lat_units.push_back("degrees_north");
        lat_units.push_back("degree_north");
        lat_units.push_back("degree_N");
        lat_units.push_back("degrees_N");
        lat_units.push_back("degreeN");
        lat_units.push_back("degreesN");
    }
    static vector<string> lon_units;
    if (lon_units.empty()) {
        lon_units.push_back("degrees_east");
        lon_units.push_back("degree_east");
        lon_units.push_back("degree_E");
        lon_units.push_back("degrees_E");
        lon_units.push_back("degreeE");
        lon_units.push_back("degreesE");
    }
    static vector<string> vert_units;
    if (vert_units.empty()) {
        vert_units.push_back("bar");
        vert_units.push_back("millibar");
        vert_units.push_back("decibar");
        vert_units.push_back("atm"); // atmosphere
        vert_units.push_back("Pa"); // pascal
        vert_units.push_back("hPa"); // hectopascal
        vert_units.push_back("meter");
        vert_units.push_back("metre");
        vert_units.push_back("m"); // meter
        vert_units.push_back("kilometer");
        vert_units.push_back("km"); // kilometer
        // deprecated
        vert_units.push_back("level");
        vert_units.push_back("layer");
        vert_units.push_back("sigma_level");
    }

    vector<Variable*> vars = get_vars();
    vector<Variable*>::iterator var_it;

    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        //cerr << "Attempting to decorate " << var->get_name() << endl;
        Attribute *att;
        if ((att = var->find_att("bounds"))) {
            string val = att->get_string();
            Variable *bound_var;
            if ((bound_var = find_var(val))) {
                vector<Variable*>::iterator it;
                it = find(vars.begin(), vars.end(), bound_var);
                (*it) = new BoundaryVariable(bound_var, var);
            }
        }
        if ((att = var->find_att("units"))) {
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
        if ((att = var->find_att("standard_name"))) {
            string val = att->get_string();
            if ("latitude" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            } else if ("longitude" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
        }
        if ((att = var->find_att("axis"))) {
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
        if ((att = var->find_att("positive"))) {
            string val = att->get_string();
            transform(val.begin(), val.end(), val.begin(), tolower);
            if ("up" == val || "down" == val) {
                (*var_it) = new CoordinateVariable(var);
                continue;
            }
        }
        if (var->get_name() == "cell_neighbors") {
            Dimension *dim = find_dim("cells");
            if (dim) {
                (*var_it) = new ConnectivityVariable(var, dim);
            }
        }
        if (var->get_name() == "cell_corners") {
            Dimension *dim = find_dim("cellcorners");
            if (dim) {
                (*var_it) = new ConnectivityVariable(var, dim);
            }
        }
        if (var->get_name() == "cell_edges") {
            Dimension *dim = find_dim("celledges");
            if (dim) {
                (*var_it) = new ConnectivityVariable(var, dim);
            }
        }
    }
#ifdef DEBUG
    cerr << "Variables after decoration" << endl;
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        cerr << (*var_it) << endl;
    }
#endif // DEBUG

    decorate_set(vars);
}

