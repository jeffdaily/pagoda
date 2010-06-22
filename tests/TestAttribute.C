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
#include "pagoda.H"


int main(int argc, char **argv)
{
    pagoda::initialize(&argc,&argv);

    if (argc != 2) {
        cout << "usage: TestAttribute <filename>" << endl;
        return EXIT_FAILURE;
    }

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
        string test;
        att->get_values()->as(test);
        cout << test << endl;
        cout << endl;
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

    pagoda::finalize();

    return EXIT_SUCCESS;
}
