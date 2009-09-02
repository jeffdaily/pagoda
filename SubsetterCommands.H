#ifndef SUBSETTERCOMMANDS_H_
#define SUBSETTERCOMMANDS_H_

#include <string>
#include <vector>

#include "LatLonBox.H"
#include "Slice.H"

using std::string;
using std::vector;


class SubsetterCommands
{
    public:
        SubsetterCommands();
        SubsetterCommands(int argc, char **argv);
        SubsetterCommands(const SubsetterCommands &that);
        SubsetterCommands& operator = (const SubsetterCommands &that);
        virtual ~SubsetterCommands();

        void parse(int argc, char **argv);

        const string& get_usage() const;
        const vector<string>& get_intput_filenames() const;
        const string& get_output_filename() const;
        const string& get_join_name() const;
        bool has_box() const;
        const LatLonBox& get_box() const;
        const vector<DimSlice>& get_slices() const;

    protected:
        vector<string> input_filenames;
        string output_filename;
        string join_name;
        vector<DimSlice> slices;
        bool _has_box;
        LatLonBox box;

        
};

#endif // SUBSETTERCOMMANDS_H_
