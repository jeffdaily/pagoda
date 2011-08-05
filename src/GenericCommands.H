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
class GenericAttribute;
class Variable;


/**
 * Generic command-line parsing for all tools.
 */
class GenericCommands
{
    public:
        GenericCommands();
        GenericCommands(int argc, char **argv);
        virtual ~GenericCommands();

        virtual void parse(int argc, char **argv);

        virtual void get_inputs_and_outputs(Dataset* &dataset,
                                            vector<Variable*> &vars,
                                            FileWriter* &writer);
        virtual void get_inputs(Dataset* &dataset,
                                vector<Variable*> &vars,
                                vector<Dimension*> &dims,
                                vector<Attribute*> &atts);
        virtual FileWriter* get_output() const;

        virtual Dataset* get_dataset();
        virtual vector<Variable*> get_variables(Dataset *dataset);
        virtual vector<Dimension*> get_dimensions(Dataset *dataset);
        virtual vector<Attribute*> get_attributes(Dataset *dataset);

        virtual vector<LatLonBox> get_boxes() const;
        virtual vector<IndexHyperslab> get_index_hyperslabs() const;
        virtual vector<CoordHyperslab> get_coord_hyperslabs() const;
        virtual vector<string> get_input_filenames() const;
        virtual string get_output_filename() const;
        virtual set<string> get_variables() const;
        virtual string get_join_name() const;
        virtual bool is_excluding_variables() const;
        virtual bool is_alphabetizing() const;
        virtual bool is_processing_all_coords() const;
        virtual bool is_processing_coords() const;
        virtual bool is_modifying_history() const;
        virtual bool is_appending() const;
        virtual bool is_overwriting() const;
        virtual bool is_fixing_record_dimension() const;
        virtual bool is_nonblocking() const;
        virtual bool is_reading_all_records() const;
        virtual bool is_reading_all_variables() const;
        virtual int get_header_pad() const;
        virtual int get_number_of_groups() const;
        virtual string get_usage() const;
        virtual string get_version() const;
        virtual FileFormat get_file_format() const;
        virtual string get_input_path() const;

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
        int number_of_groups;
        vector<IndexHyperslab> index_hyperslabs;
        vector<CoordHyperslab> coord_hyperslabs;
        vector<LatLonBox> boxes;
        FileFormat file_format;
        bool nonblocking_io;
        bool reading_all_records;
        bool reading_all_variables;
        vector<GenericAttribute*> histories;
};

#endif // GENERICCOMMANDS_H_
