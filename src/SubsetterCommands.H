#ifndef SUBSETTERCOMMANDS_H_
#define SUBSETTERCOMMANDS_H_

#include <string>
#include <vector>

#include "CommandLineParser.H"
#include "LatLonBox.H"
#include "Slice.H"

using std::string;
using std::vector;

class Dataset;
class FileWriter;


/**
 * All command-line parsing for the subsetter.
 */
class SubsetterCommands : public CommandLineParser
{
    public:
        SubsetterCommands();
        SubsetterCommands(int argc, char **argv);
        virtual ~SubsetterCommands();

        virtual void parse(int argc, char **argv);

        Dataset* get_dataset() const;
        FileWriter* get_output() const;
        LatLonBox get_box() const;
        vector<DimSlice> get_slices() const;

        vector<string> get_input_filenames() const;
        string get_output_filename() const;
        string get_join_name() const;

    protected:
        void init();

        vector<string> input_filenames;
        string output_filename;
        string join_name;
        vector<DimSlice> slices;
        bool _has_box;
        LatLonBox box;
};

#endif // SUBSETTERCOMMANDS_H_
