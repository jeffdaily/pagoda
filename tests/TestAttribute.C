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
    Dataset *dataset;
    vector<Attribute*> atts;
    vector<Variable*> vars;

    pagoda::initialize(&argc,&argv);

    if (argc != 2) {
        cout << "usage: TestAttribute <filename>" << endl;
        return EXIT_FAILURE;
    }

    for (int argi=0; argi<argc; ++argi) {
        cout << argv[argi] << endl;
    }

    dataset = Dataset::open(argv[1]);

    cout << endl << "Test as(string)" << endl << endl;

    cout << "@@@@@@@@@@@@@@@@@@@ GLOBAL ATTRIBUTES" << endl;
    atts = dataset->get_atts();
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
    vars = dataset->get_vars();
    for (size_t varid=0,nvar=vars.size(); varid<nvar; ++varid) {
        Variable *var = vars[varid];
        cout << var->get_name() << endl;
        atts = var->get_atts();
        for (int attid=0,natt=atts.size(); attid<natt; ++attid) {
            cout << "\t" << atts.at(attid) << endl;
        }
    }

    cout << "@@@@@@@@@@@@@@@@@@@ GLOBAL ATTRIBUTES" << endl;
    atts = dataset->get_atts();
    for (size_t attid=0,natt=atts.size(); attid<natt; ++attid) {
        Attribute *att = atts[attid];
        string test;

        cout << att << endl;
        cout << "TEST OF as(), size=" << att->get_count() << endl;
        att->get_values()->as(test);
        cout << test << endl;
        cout << endl;
    }

    cout << "@@@@@@@@@@@@@@@@@@@ VARIABLE ATTRIBUTES" << endl;
    vars = dataset->get_vars();
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
