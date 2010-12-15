#ifdef HAVE_CONFIG_H
#   include "config.h"
#endif // HAVE_CONFIG_H

// C++ includes, std and otherwise
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

// C++ includes
#include "AggregationJoinExisting.H"
#include "Attribute.H"
#include "Bootstrap.H"
#include "Dimension.H"
#include "PnetcdfDataset.H"
#include "Util.H"
#include "Variable.H"

using std::cout;
using std::endl;
using std::string;
using std::vector;


int main(int argc, char **argv)
{
    string s("        ");

    pagoda::initialize(&argc, &argv);

    if (argc < 3) {
        if (0 == pagoda::me) {
            cout << "Usage: TestUnion dim_name file1 <file2> ..." << endl;
        }
        pagoda::finalize();
        return EXIT_FAILURE;
    }

    if (0 == pagoda::me) {
        cout << "joined dataset" << endl;
    }
    Aggregation *dataset = new AggregationJoinExisting(argv[1]);
    for (int argi=2; argi<argc; ++argi) {
        dataset->add(new PnetcdfDataset(argv[argi]));
    }

    if (0 == pagoda::me) {
        cout << "dimensions:" << endl;
    }
    vector<Dimension*> dims = dataset->get_dims();
    for (size_t dimid=0,limit=dims.size(); dimid<limit; ++dimid) {
        Dimension *dim = dims[dimid];
        if (0 == pagoda::me) {
            cout << s << dim->get_name() << " = ";
            if (dim->is_unlimited()) {
                cout << "UNLIMITED ; // (" << dim->get_size() << " currently)" << endl;
            } else {
                cout << dim->get_size() << " ;" << endl;
            }
        }
    }

    if (0 == pagoda::me) {
        cout << "variables:" << endl;
    }
    vector<Variable*> vars = dataset->get_vars();
    for (size_t varid=0,limit=vars.size(); varid<limit; ++varid) {
        Variable *var = vars[varid];
        if (0 == pagoda::me) {
            cout << s << var->get_type() << " " << var->get_name() << "(";
        }
        vector<Dimension*> dims = var->get_dims();
        for (size_t dimid=0,limit=dims.size()-1; dimid<limit; ++dimid) {
            Dimension *dim = dims[dimid];
            if (0 == pagoda::me) {
                cout << dim->get_name() << ", ";
            }
        }
        Dimension *dim = dims[dims.size()-1];
        if (0 == pagoda::me) {
            cout << dim->get_name() << ") ;" << endl;
        }
        vector<Attribute*> atts = var->get_atts();
        for (size_t attid=0,limit=atts.size(); attid<limit; ++ attid) {
            Attribute *att = atts[attid];
            if (0 == pagoda::me) {
                cout << s << s << att << endl;
            }
        }
    }

    if (0 == pagoda::me) {
        cout << "// global attributes:" << endl;
    }
    vector<Attribute*> atts = dataset->get_atts();
    for (size_t attid=0,limit=atts.size(); attid<limit; ++attid) {
        if (0 == pagoda::me) {
            cout << s << s << atts[attid] << endl;
        }
    }

    // Must close netcdf file -- in dtor.
    delete dataset;

    // Must always call these to exit cleanly.
    pagoda::finalize();

    return EXIT_SUCCESS;
}

