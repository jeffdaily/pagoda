#ifdef HAVE_CONFIG_H
#include "config.h"
#endif // HAVE_CONFIG_H

// C includes, std and otherwise
#include <ga.h>
#include <macdecls.h>
#include <mpi.h>
#include <pnetcdf.h>
#include <unistd.h>

// C++ includes, std and otherwise
#include <iostream>
using std::cout;
using std::endl;
#include <sstream>
using std::ostringstream;
#include <string>
using std::string;
#include <vector>
using std::vector;

// C++ includes
#include "Attribute.H"
#include "NetcdfAttribute.H"
#include "NetcdfDataset.H"
#include "NetcdfVariable.H"
#include "Util.H"
#include "Values.H"
#include "Variable.H"


int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    GA_Initialize();

    if (argc < 2) return EXIT_FAILURE;

    for (int argi=0; argi<argc; ++argi) {
        cout << argv[argi] << endl;
    }

    NetcdfDataset *dataset = new NetcdfDataset(argv[1]);

    cout << "@@@@@@@@@@@@@@@@@@@ GLOBAL ATTRIBUTES" << endl;
    vector<Attribute*> atts = dataset->get_atts();
    for (size_t attid=0,natt=atts.size(); attid<natt; ++attid) {
        Attribute *att = atts[attid];
        cout << att << endl;

        cout << "TEST OF as(), size=" << att->get_count() << endl;
        char *test;
        att->get_values()->as(test);
        for (size_t i=0,limit=att->get_count(); i<limit; ++i) {
            cout << test[i] << " ";
        }
        cout << endl;
        delete [] test;
    }

    cout << "@@@@@@@@@@@@@@@@@@@ VARIABLE ATTRIBUTES" << endl;
    vector<Variable*> vars = dataset->get_vars();
    for (size_t varid=0,nvar=vars.size(); varid<nvar; ++varid) {
        Variable *var = vars[varid];
        cout << var->get_name() << endl;
        atts = var->get_atts();
        for (int attid=0,natt=atts.size(); attid<natt; ++attid) {
            cout << "\t" << atts.at(attid) << endl;
        }
    }

    delete dataset;

    // Must always call these to exit cleanly.
    GA_Terminate();
    MPI_Finalize();

    return EXIT_SUCCESS;
}

