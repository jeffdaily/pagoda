#ifdef HAVE_CONFIG_H
#   include "config.h"
#endif // HAVE_CONFIG_H

// C++ includes, std and otherwise
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using std::cout;
using std::endl;
using std::ostringstream;
using std::string;
using std::vector;

// C++ includes
#include "Attribute.H"
#include "Bootstrap.H"
#include "Dimension.H"
#include "NetcdfDataset.H"
#include "SubsetterException.H"
#include "Util.H"
#include "Variable.H"


int main(int argc, char **argv)
{
    string s("        ");
    Dataset *dataset;
    vector<Dimension*> dims;
    vector<Variable*> vars;
    vector<Attribute*> atts;

    pagoda::initialize(&argc, &argv);

    if (argc < 2) {
        if (0 == pagoda::me) {
            cout << "Usage: TestNetcdfDataset file.nc" << endl;
        }
        pagoda::finalize();
        return EXIT_FAILURE;
    }


    cout << "netcdf " << argv[1] << endl;
    dataset = new NetcdfDataset(argv[1]);

    cout << "dimensions:" << endl;
    dims = dataset->get_dims();
    for (size_t dimid=0,limit=dims.size(); dimid<limit; ++dimid) {
        Dimension *dim = dims[dimid];
        cout << s << dim->get_name() << " = " << dim->get_size() << " ;" << endl;
    }

    cout << "variables:" << endl;
    vars = dataset->get_vars();
    for (size_t varid=0,limit=vars.size(); varid<limit; ++varid) {
        Variable *var = vars[varid];
        vector<Dimension*> dims = var->get_dims();
        vector<Attribute*> atts = var->get_atts();

        cout << s << var->get_type() << " " << var->get_name();
        if (!dims.empty()) {
            cout << "(";
            for (size_t dimid=0,limit=dims.size()-1; dimid<limit; ++dimid) {
                Dimension *dim = dims[dimid];
                cout << dimid << " " <<  dim->get_name() << ", ";
            }
            cout << dims.back()->get_name();
            cout << ") ;" << endl;
        }

        for (size_t attid=0,limit=atts.size(); attid<limit; ++ attid) {
            Attribute *att = atts[attid];
            cout << s << s << att << endl;
        }
    }

    cout << "// global attributes:" << endl;
    atts = dataset->get_atts();
    for (size_t attid=0,limit=atts.size(); attid<limit; ++attid) {
        cout << s << s << atts[attid] << endl;
    }

    // Must close netcdf file -- in dtor.
    delete dataset;

    // Must always call these to exit cleanly.
    pagoda::finalize();

    return EXIT_SUCCESS;
}
