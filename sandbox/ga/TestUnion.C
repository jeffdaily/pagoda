#ifdef HAVE_CONFIG_H
#   include "config.h"
#endif /* HAVE_CONFIG_H */

// C includes, std and otherwise
#include <ga.h>
#include <macdecls.h>
#include <mpi.h>
#include <pnetcdf.h>

// C++ includes, std and otherwise
#include <iostream>
#include <string>
#include <vector>

// C++ includes
#include "AggregationUnion.H"
#include "Attribute.H"
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
    MPI_Init(&argc, &argv);
    GA_Initialize();

    int me = GA_Nodeid();
    string s("        ");

    if (argc < 2) return EXIT_FAILURE;

    if (0 == me) {
        for (int argi=0; argi<argc; ++argi) {
            cout << argv[argi] << endl;
        }
    }

    if (0 == me) {
        cout << "union dataset" << endl;
    }
    Aggregation *dataset = new AggregationUnion;
    for (int argi=1; argi<argc; ++argi) {
        dataset->add(new PnetcdfDataset(argv[argi]));
    }

    if (0 == me) {
        cout << "dimensions:" << endl;
        vector<Dimension*> dims = dataset->get_dims();
        for (size_t dimid=0,limit=dims.size(); dimid<limit; ++dimid) {
            Dimension *dim = dims[dimid];
            cout << s << dim->get_name() << " = " << dim->get_size() << " ;" << endl;
        }

        cout << "variables:" << endl;
        vector<Variable*> vars = dataset->get_vars();
        for (size_t varid=0,limit=vars.size(); varid<limit; ++varid) {
            Variable *var = vars[varid];
            cout << s << var->get_type() << " " << var->get_name() << "(";
            vector<Dimension*> dims = var->get_dims();
            for (size_t dimid=0,limit=dims.size()-1; dimid<limit; ++dimid) {
                Dimension *dim = dims[dimid];
                cout << dim->get_name() << ", ";
            }
            Dimension *dim = dims[dims.size()-1];
            cout << dim->get_name() << ") ;" << endl;
            vector<Attribute*> atts = var->get_atts();
            for (size_t attid=0,limit=atts.size(); attid<limit; ++ attid) {
                Attribute *att = atts[attid];
                cout << s << s << att << endl;
            }
        }

        cout << "// global attributes:" << endl;
        vector<Attribute*> atts = dataset->get_atts();
        for (size_t attid=0,limit=atts.size(); attid<limit; ++attid) {
            cout << s << s << atts[attid] << endl;
        }
    }

    // Must close netcdf file -- in dtor.
    delete dataset;

    // Must always call these to exit cleanly.
    GA_Terminate();
    MPI_Finalize();

    return EXIT_SUCCESS;
}

