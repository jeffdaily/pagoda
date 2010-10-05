#ifndef ABSTRACTDATASET_H_
#define ABSTRACTDATASET_H_

#include <iostream>
#include <string>
#include <vector>

#include "Dataset.H"
#include "FileFormat.H"
#include "LatLonBox.H"
#include "Slice.H"

using std::ostream;
using std::string;
using std::vector;

class Attribute;
class Dimension;
class Grid;
class MaskMap;
class Variable;


/**
 * Open a file and access the Attributes, Variables, and Dimensions within.
 *
 * As much as can be implemented abstractly is done here.
 */
class AbstractDataset : public Dataset
{
    public:
        virtual ~AbstractDataset();

        virtual void close() = 0;

        virtual vector<Attribute*> get_atts() const = 0;
        virtual vector<Dimension*> get_dims() const = 0;
        virtual vector<Variable*> get_vars() const = 0;

        virtual vector<Grid*> get_grids();
        virtual Grid* get_grid();

        virtual Attribute* get_att(const string &name,
                bool ignore_case=false, bool within=false) const;
        virtual Attribute* get_att(const vector<string> &names,
                bool ignore_case=false, bool within=false) const;

        virtual Dimension* get_dim(const string &name,
                bool ignore_case=false, bool within=false) const;

        virtual Variable* get_var(const string &name,
                bool ignore_case=false, bool within=false) const;

        virtual void set_masks(MaskMap *masks);
        virtual void push_masks(MaskMap *masks);
        virtual MaskMap* pop_masks();
        virtual MaskMap* get_masks() const;

        virtual void wait() = 0;

        virtual FileFormat get_file_format() const = 0;

        virtual ostream& print(ostream &os) const = 0;

    protected:
        AbstractDataset();

        vector<Grid*> grids;
        vector<MaskMap*> masks;
};

#endif // ABSTRACTDATASET_H_
