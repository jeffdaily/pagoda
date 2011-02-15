/**
 * @file pgdump.C
 *
 * Replicate the functionality of ncdump/ncmpidump while supporting
 * aggregation.
 *
 * We wouldn't normally need yet another ncdump, but this program will allow
 * any supported data format to be dumped as well as support aggregation
 * through (hopefully) NcML or some other form of aggregation.
 */
#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;

#include "Attribute.H"
#include "Bootstrap.H"
#include "CommandLineOption.H"
#include "CommandLineParser.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Print.H"
#include "Values.H"
#include "Variable.H"


static CommandLineOption TYPE(0, "type", false,
        "print the type of file to stdout");
static CommandLineOption HAS_RECORD(0, "has_record", false,
        "exit with 0 if file has a record dimension > 0");
static CommandLineOption HELP('h', "help", false,
        "print usage and exit");

static int dump_type(const string &filename);
static int has_record(const string &filename);
static int dump_header(const string &filename);


int main(int argc, char **argv)
{
    CommandLineParser parser;
    string filename;
    int status;

    pagoda::initialize(&argc,&argv);

    parser.push_back(&TYPE);
    parser.push_back(&HAS_RECORD);
    parser.push_back(&HELP);
    parser.parse(argc,argv);

    if (parser.get_positional_arguments().empty() || parser.count("help")) {
        pagoda::println_zero("Usage: pgdump [--type] <filename>");
        pagoda::println_zero(parser.get_usage());
        pagoda::finalize();
        if (parser.count("help")) {
            return EXIT_SUCCESS;
        } else {
            return EXIT_FAILURE;
        }
    } else {
        filename = parser.get_positional_arguments().back();
    }

    if (!pagoda::file_exists(filename)) {
        pagoda::println_zero("pgdump: file not found: " + filename);
        pagoda::finalize();
        return EXIT_FAILURE;
    }

    if (parser.count("type")) {
        status = dump_type(filename);
    } else if (parser.count("has_record")) {
        status = has_record(filename);
    } else {
        status = dump_header(filename);
    }

    pagoda::finalize();

    return status;
}


static int dump_type(const string &filename)
{
    Dataset *dataset = Dataset::open(filename);
    FileFormat format = dataset->get_file_format();
    string name;
    switch (format) {
        case FF_NETCDF4: name = "NC4"; break;
        case FF_NETCDF4_CLASSIC: name = "NC4_CLASSIC"; break;
        case FF_CDF1: name = "CDF1"; break;
        case FF_CDF2: name = "CDF2"; break;
        case FF_CDF5: name = "CDF5"; break;
        case FF_UNKNOWN:
        default: name = "UNKNOWN"; break;
    }
    delete dataset;
    pagoda::println_zero(name);
    return EXIT_SUCCESS;
}


/* return EXIT_SUCCESS if the file has at least one record */
static int has_record(const string &filename)
{
    Dataset *dataset = Dataset::open(filename);
    int status = EXIT_FAILURE;
    Dimension *dim;

    if (NULL != (dim = dataset->get_udim())) {
        if (dim->get_size() > 0) {
            status = EXIT_SUCCESS;
        }
    }
    
    delete dataset;

    return status;
}


static int dump_header(const string &filename)
{
    Dataset *dataset;
    vector<Attribute*> atts;
    vector<Dimension*> dims;
    vector<Variable*> vars;
    vector<Attribute*>::const_iterator att_it,att_end;
    vector<Dimension*>::const_iterator dim_it,dim_end;
    vector<Variable*>::const_iterator var_it,var_end;

    dataset = Dataset::open(filename);
    atts = dataset->get_atts();
    dims = dataset->get_dims();
    vars = dataset->get_vars();

    cout << filename << " {" << endl;

    cout << "dimensions:" << endl;
    for (dim_it=dims.begin(),dim_end=dims.end(); dim_it!=dim_end; ++dim_it) {
        Dimension *dim = *dim_it;

        cout << "\t";
        cout << dim->get_name() << " = ";
        if (dim->is_unlimited()) {
            cout << "UNLIMITED ; // (" << dim->get_size() << " currently)";
        }
        else {
            cout << dim->get_size() << " ;";
        }
        cout << endl;
    }

    cout << "variables:" << endl;
    for (var_it=vars.begin(),var_end=vars.end(); var_it!=var_end; ++var_it) {
        Variable *var = *var_it;
        vector<Attribute*> var_atts = var->get_atts();
        vector<Dimension*> var_dims = var->get_dims();

        cout << "\t";
        cout << var->get_type() << " " << var->get_name() << "(";
        dim_it = var_dims.begin();
        dim_end = var_dims.end();
        if (dim_it != dim_end) {
            cout << (*dim_it)->get_name();
            ++dim_it;
        }
        for (/*empty*/; dim_it!=dim_end; ++dim_it) {
            cout << ", " << (*dim_it)->get_name();
        }
        cout << ") ;" << endl;

        for (att_it=var_atts.begin(),att_end=var_atts.end();
                att_it!=att_end; ++att_it) {
            Attribute *att = *att_it;

            cout << "\t\t" << var->get_name() << ":" << att->get_name();
            cout << " = \"" << att->get_string() << "\" ;" << endl;
        }
    }

    cout << endl << "// global attributes:" << endl;
    for (att_it=atts.begin(),att_end=atts.end(); att_it!=att_end; ++att_it) {
        Attribute *att = *att_it;

        cout << "\t\t:" << att->get_name() << " = \"";
        cout << att->get_string() << "\" ;" << endl;
    }
    cout << "}" << endl;

    delete dataset;

    return EXIT_SUCCESS;
}
