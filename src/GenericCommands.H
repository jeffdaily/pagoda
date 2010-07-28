#ifndef GENERICCOMMANDS_H_
#define GENERICCOMMANDS_H_

#include <set>
#include <string>
#include <vector>

#include "CommandLineParser.H"
#include "LatLonBox.H"
#include "Slice.H"

using std::set;
using std::string;
using std::vector;

class Attribute; class Dataset;
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

        Dataset* get_dataset() const;
        FileWriter* get_output() const;
        vector<Variable*> get_variables(Dataset *dataset) const;
        vector<Dimension*> get_dimensions(Dataset *dataset) const;
        vector<Attribute*> get_attributes(Dataset *dataset) const;

        vector<LatLonBox> get_boxes() const;
        vector<DimSlice> get_slices() const;
        vector<string> get_input_filenames() const;
        string get_output_filename() const;
        set<string> get_variables() const;
        bool get_exclude() const;
        string get_join_name() const;
        bool get_alphabetize() const;
        bool get_all_coords() const;
        bool get_process_coords() const;
        bool get_modify_history() const;
        bool get_help() const;
        string get_usage()  const;

    protected:
        void init();

        CommandLineParser parser;
        string cmdline;
        vector<string> input_filenames;
        string output_filename;
        set<string> variables;
        bool exclude;
        string join_name;
        bool alphabetize;
        bool all_coords;
        bool process_coords;
        bool modify_history;
        bool process_topology;
        bool help;
        vector<DimSlice> slices;
        vector<LatLonBox> boxes;
};

#endif // GENERICCOMMANDS_H_
