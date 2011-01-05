#ifndef DATASET_H_
#define DATASET_H_

#include <iostream>
#include <string>
#include <vector>

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
 * Open a file and access the Attribute, Variable, and Dimension instances
 * within.
 */
class Dataset
{
    public:
        typedef Dataset*(*opener_t)(const string &);

        /**
         * Opens the given filename and deduces the appropriate type handler.
         *
         * @param[in] filename the name of the file to open
         * @return the Dataset
         */
        static Dataset* open(const string &filename);

        /**
         * Registers a file format handler.
         */
        static void register_opener(opener_t opener);

        /**
         * Destructs the Dataset.
         */
        virtual ~Dataset();

        /**
         * Explicitly closes the Dataset.
         *
         * This should also happen automatically if needed when the Dataset is
         * destructed.
         */
        virtual void close() = 0;

        /**
         * Returns all global Attribute instances in this Dataset.
         *
         * @return all global Attribute instances in this Dataset.
         */
        virtual vector<Attribute*> get_atts() const = 0;

        /**
         * Returns all Dimension instances in this Dataset.
         *
         * @return all Dimension instances in this Dataset.
         */
        virtual vector<Dimension*> get_dims() const = 0;

        /**
         * Returns all Variable instances in this Dataset.
         *
         * @return all Variable instances in this Dataset.
         */
        virtual vector<Variable*> get_vars() const = 0;

        /**
         * Returns all Grid instances found in this Dataset.
         *
         * @return all Grid instances found in this Dataset
         */
        virtual vector<Grid*> get_grids() = 0;

        /**
         * Returns the first Grid found in this Dataset.
         *
         * @return the first Grid found in this Dataset
         */
        virtual Grid* get_grid() = 0;

        /**
         * Returns the Attribute with the given name.
         *
         * @param[in] name the name of the Attribute
         * @param[in] ignore_case whether the search is case-sensitive
         * @param[in] within whether to search for a substring
         * @returns NULL if not found, otherwise the Attribute
         */
        virtual Attribute* get_att(const string &name,
                                   bool ignore_case=false,
                                   bool within=false) const = 0;

        /**
         * Returns the Attribute with any of the given names.
         *
         * @param[in] names the possible names of the Attribute
         * @param[in] ignore_case whether the search is case-sensitive
         * @param[in] within whether to search for a substring
         * @returns NULL if not found, otherwise the Attribute
         */
        virtual Attribute* get_att(const vector<string> &names,
                                   bool ignore_case=false,
                                   bool within=false) const = 0;

        /**
         * Returns the Dimension with the given name.
         *
         * @param[in] name the name of the Dimension
         * @param[in] ignore_case whether the search is case-sensitive
         * @param[in] within whether to search for a substring
         * @returns NULL if not found, otherwise the Dimension
         */
        virtual Dimension* get_dim(const string &name,
                                   bool ignore_case=false,
                                   bool within=false) const = 0;

        /**
         * Returns the unlimited (aka record) Dimension.
         *
         * @returns NULL if no record Dimension is present
         */
        virtual Dimension* get_udim() const = 0;

        /**
         * Returns the Variable with the given name.
         *
         * @param[in] name the name of the Variable
         * @param[in] ignore_case whether the search is case-sensitive
         * @param[in] within whether to search for a substring
         * @returns NULL if not found, otherwise the Variable
         */
        virtual Variable* get_var(const string &name,
                                  bool ignore_case=false,
                                  bool within=false) const = 0;

        /**
         * Replaces existing MaskMap instances with the given one.
         *
         * @param[in] masks will replace any existing MaskMap instances
         */
        virtual void set_masks(MaskMap *masks) = 0;

        /**
         * Makes the given MaskMap the current Mask collection to use.
         *
         * This is useful for temporarily suspending the automatic subsetting
         * which occurs during Variable::read() operations.  For example,
         * @code
         * Dataset *dataset = Dataset::open(filename);
         * Variable *variable = dataset->get_var("varname");
         * dataset->push_masks(NULL);
         * Array *array = variable->read();
         * dataset->pop_masks();
         * @endcode
         *
         * @param[in] masks will become the current MaskMap while preserving any
         * existing ones
         */
        virtual void push_masks(MaskMap *masks) = 0;

        /**
         * Removes and returns the most recent MaskMap from the top of the
         * stack.
         *
         * @return the most recent MaskMap from the top of the stack
         */
        virtual MaskMap* pop_masks() = 0;

        /**
         * Returns the MaskMap collection for this Dataset.
         *
         * @return the MaskMap collection for this Dataset
         */
        virtual MaskMap* get_masks() const = 0;

        /**
         * Completes a non-blocking IO operation.
         *
         * Non-blocking IO occurs within the context of a Dataset.  Although
         * the non-blocking reads occur on the Variable instances via
         * Variable::iread(), the Dataset waits for their termination.
         */
        virtual void wait() = 0;

        /**
         * Returns the FileFormat of this Dataset.
         *
         * @return the FileFormat of this Dataset
         */
        virtual FileFormat get_file_format() const = 0;

        /**
         * Returns the approximate number of bytes this Dataset consumes.
         * 
         * @return the approximate number of bytes this Dataset consumes
         */
        virtual int64_t get_bytes() const = 0;

        /**
         * Writes the Dataset to the given stream.
         *
         * Useful for debugging.  This is the virtual friend function idiom.
         * http://www.parashift.com/c++-faq-lite/input-output.html#faq-15.11
         *
         * @param[in] os the stream
         * @return the stream
         */
        virtual ostream& print(ostream &os) const = 0;

        /**
         * Writes the Dataset to the given stream using virtual print().
         *
         * @param[in] os the stream
         * @param[in] dataset Dataset to write
         * @return the stream
         */
        friend ostream& operator << (ostream &os, const Dataset *dataset);

        /**
         * Compares two Dataset instances for equality.
         *
         * Dataset instances are equal if the set of Dimension and Variable
         * instances match.  Dimensions and Variables need not be in the same
         * order.
         *
         * @param[in] left Dataset to compare
         * @param[in] right Dataset to compare
         */
        static bool equal(const Dataset *left, const Dataset *right);

    protected:
        /**
         * Constructs the Dataset abstract base class.
         */
        Dataset();

    private:
        static vector<opener_t> openers;
};

#endif // DATASET_H_
