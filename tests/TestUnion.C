#ifdef HAVE_CONFIG_H
#   include "config.h"
#endif /* HAVE_CONFIG_H */

// C++ includes, std and otherwise
#include <iostream>
#include <string>
#include <vector>

// C++ includes
#include "AggregationUnion.H"
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
    Aggregation *dataset;

    pagoda::initialize(&argc, &argv);

    if (argc < 2) {
        if (0 == pagoda::me) {
            cout << "Usage: TestUnion file1 [file2] ..." << endl;
        }
        pagoda::finalize();
        return EXIT_FAILURE;
    }

    if (0 == pagoda::me) {
        cout << "union dataset" << endl;
    }
    dataset = new AggregationUnion;
    for (int argi=1; argi<argc; ++argi) {
        dataset->add(Dataset::open(argv[argi]));
    }

    if (0 == pagoda::me) {
        vector<Dimension*> dims = dataset->get_dims();
        vector<Variable*> vars = dataset->get_vars();
        vector<Attribute*> atts = dataset->get_atts();

        cout << "dimensions:" << endl;
        for (size_t dimid=0,limit=dims.size(); dimid<limit; ++dimid) {
            Dimension *dim = dims[dimid];
            cout << s << dim->get_name() << " = " << dim->get_size() << " ;" << endl;
        }

        cout << "variables:" << endl;
        for (size_t varid=0,limit=vars.size(); varid<limit; ++varid) {
            Variable *var = vars[varid];
            vector<Dimension*> dims = var->get_dims();
            vector<Attribute*> atts = var->get_atts();

            cout << s << var->get_type() << " " << var->get_name();
            if (!dims.empty()) {
                cout << "(";
                for (size_t dimid=0,limit=dims.size()-1; dimid<limit; ++dimid) {
                    Dimension *dim = dims[dimid];
                    cout << dim->get_name() << ", ";
                }
                cout << dims.back()->get_name() << ") ;" << endl;
            }

            for (size_t attid=0,limit=atts.size(); attid<limit; ++ attid) {
                Attribute *att = atts[attid];
                cout << s << s << att << endl;
            }
        }

        cout << "// global attributes:" << endl;
        for (size_t attid=0,limit=atts.size(); attid<limit; ++attid) {
            cout << s << s << atts[attid] << endl;
        }
    }

    // Must close netcdf file -- in dtor.
    delete dataset;

    // Must always call these to exit cleanly.
    pagoda::finalize();

    return EXIT_SUCCESS;
}
