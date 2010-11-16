#ifndef PNETCDFDATASET_H_
#define PNETCDFDATASET_H_

#include <pnetcdf.h>

#include <string>
#include <vector>

using std::string;
using std::vector;

#include "AbstractDataset.H"
#include "FileFormat.H"

class Array;
class Attribute;
class Dimension;
class PnetcdfAttribute;
class PnetcdfDimension;
class PnetcdfVariable;
class Variable;


/**
 * The pnetcdf implementation of the Dataset.
 *
 * A PnetcdfDataset instance owns the Variable, Dimension, and Attribute
 * instances which are accessed via its methods.  Do not explicitly delete
 * them.
 */
class PnetcdfDataset : public AbstractDataset
{
    public:
        /**
         * Constructs a PnetcdfDataset by opening the given filename.
         *
         * @param[in] filename to open
         */
        PnetcdfDataset(const string &filename);

        /**
         * Destructs and automatically closes the PnetcdfDataset.
         */
        virtual ~PnetcdfDataset();

        virtual void close();

        virtual vector<Attribute*> get_atts() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Variable*> get_vars() const;

        virtual void wait();

        virtual FileFormat get_file_format() const;

        virtual ostream& print(ostream &os) const;

        virtual Attribute* get_att(const string &name,
                                   bool ignore_case=false,
                                   bool within=false) const;
        virtual Attribute* get_att(const vector<string> &names,
                                   bool ignore_case=false,
                                   bool within=false) const;

        /**
         * Returns the PnetcdfAttribute at index i.
         *
         * @param[in] i the index
         * @return the PnetcdfAttribute at index i
         */
        PnetcdfAttribute* get_att(size_t i) const;

        virtual Dimension* get_dim(const string &name,
                                   bool ignore_case=false,
                                   bool within=false) const;

        /**
         * Returns the PnetcdfDimension at index i.
         *
         * @param[in] i the index
         * @return the PnetcdfDimension at index i
         */
        PnetcdfDimension* get_dim(size_t i) const;

        virtual Variable* get_var(const string &name,
                                  bool ignore_case=false,
                                  bool within=false) const;

        /**
         * Returns the PnetcdfVariable at index i.
         *
         * @param[in] i the index
         * @return the PnetcdfVariable at index i
         */
        PnetcdfVariable* get_var(size_t i) const;

        /**
         * Returns the name of the file represented by this Dataset.
         *
         * @return the name of the file represented by this Dataset
         */
        string get_filename() const;

        /**
         * Returns the "ncid" aka the NetCDF ID for this Dataset.
         *
         * @return the "ncid" aka the NetCDF ID for this Dataset
         */
        int get_id() const;

        friend class PnetcdfVariable;

    protected:
        string filename; /**< name of the file opened */
        int ncid; /**< NetCDF handle for the opened file */
        int udim; /**< NetCDF handle for the unlimited (aka record) dim */
        vector<PnetcdfAttribute*> atts; /**< all attributes */
        vector<PnetcdfDimension*> dims; /**< all dimensions */
        vector<PnetcdfVariable*> vars; /**< all variables */

        vector<int> requests; /**< non-blocking io requests */
        vector<Array*> arrays_to_release; /**< arrays associated with
                                            non-blocking IO which need to be
                                            Array::release()'d once the IO is
                                            finished */
        bool open; /**< whether this Dataset is open */
};

#endif // PNETCDFDATASET_H_
