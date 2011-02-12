#ifndef GENERICCOMMANDS_H_
#define GENERICCOMMANDS_H_

#include <set>
#include <string>
#include <vector>

#include "CommandLineOption.H"
#include "CommandLineParser.H"
#include "CoordHyperslab.H"
#include "FileFormat.H"
#include "IndexHyperslab.H"
#include "LatLonBox.H"

using std::set;
using std::string;
using std::vector;

class Attribute;
class Dataset;
class Dimension;
class FileWriter;
class Variable;


/**
 * Generic command-line parsing for all tools.
 */
class GenericCommands
{
    public:
        GenericCommands();
        GenericCommands(int argc, char **argv);
        ~GenericCommands();

        void parse(int argc, char **argv);

        void get_inputs_and_outputs(Dataset* &dataset,
                                    vector<Variable*> &vars,
                                    FileWriter* &writer);
        void get_inputs_and_outputs(Dataset* &dataset,
                                    vector<Variable*> &vars,
                                    vector<Dimension*> &dims,
                                    vector<Attribute*> &atts,
                                    FileWriter* &writer);
        void get_inputs(Dataset* &dataset,
                        vector<Variable*> &vars,
                        vector<Dimension*> &dims,
                        vector<Attribute*> &atts);
        FileWriter* get_output() const;

        Dataset* get_dataset();
        vector<Variable*> get_variables(Dataset *dataset);
        vector<Dimension*> get_dimensions(Dataset *dataset);
        vector<Attribute*> get_attributes(Dataset *dataset) const;

        vector<LatLonBox> get_boxes() const;
        vector<IndexHyperslab> get_index_hyperslabs() const;
        vector<CoordHyperslab> get_coord_hyperslabs() const;
        vector<string> get_input_filenames() const;
        string get_output_filename() const;
        set<string> get_variables() const;
        string get_join_name() const;
        bool is_excluding_variables() const;
        bool is_alphabetizing() const;
        bool is_processing_all_coords() const;
        bool is_processing_coords() const;
        bool is_modifying_history() const;
        bool is_requesting_info() const;
        bool is_appending() const;
        bool is_overwriting() const;
        bool is_fixing_record_dimension() const;
        bool is_nonblocking() const;
        int get_header_pad() const;
        string get_usage() const;
        string get_version() const;
        string get_info() const;
        FileFormat get_file_format() const;
        string get_input_path() const;

    protected:
        void init();

        CommandLineParser parser;
        string cmdline;
        vector<string> input_filenames;
        string input_path;
        string output_filename;
        set<string> variables;
        map<Dataset*,vector<Variable*> > variables_cache;
        map<Dataset*,vector<Dimension*> > dimensions_cache;
        bool exclude_variables;
        string join_name;
        bool alphabetize;
        bool process_all_coords;
        bool process_coords;
        bool modify_history;
        bool process_topology;
        bool append;
        bool overwrite;
        bool fix_record_dimension;
        int record_dimension_size;
        int header_pad;
        vector<IndexHyperslab> index_hyperslabs;
        vector<CoordHyperslab> coord_hyperslabs;
        vector<LatLonBox> boxes;
        FileFormat file_format;
        bool nonblocking_io;
};

#endif // GENERICCOMMANDS_H_
