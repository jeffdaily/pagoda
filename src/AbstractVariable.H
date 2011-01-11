#ifndef ABSTRACTVARIABLE_H_
#define ABSTRACTVARIABLE_H_

#include <stdint.h>

#include <iostream>
#include <string>
#include <vector>

#include "DataType.H"
#include "Variable.H"

using std::ostream;
using std::string;
using std::vector;

class Array;
class Attribute;
class Dataset;
class Mask;


/**
 * Abstract base class for Variable.
 *
 * Variable operations which can be implemented using Variable's own methods
 * are done here.
 */
class AbstractVariable : public Variable
{
    public:
        virtual ~AbstractVariable();

        virtual string get_name() const = 0;

        virtual vector<Dimension*> get_dims() const = 0;
        virtual bool has_record() const;
        virtual int64_t get_nrec() const;
        virtual int64_t get_ndim() const;
        virtual vector<int64_t> get_shape() const;

        virtual vector<Attribute*> get_atts() const = 0;
        virtual int64_t num_atts() const;
        virtual string get_standard_name() const;
        virtual string get_long_name() const;
        virtual bool has_fill_value(int64_t index=-1) const;
        virtual double get_fill_value(int64_t index=-1) const;
        virtual Attribute* get_att(const string &name,
                                   bool ignore_case=false) const;
        virtual Attribute* get_att(const vector<string> &names,
                                   bool ignore_case=false) const;

        virtual Dataset* get_dataset() const = 0;
        virtual vector<Mask*> get_masks() const;

        virtual DataType get_type() const = 0;

        virtual Array* read(Array *dst=NULL) const = 0;
        virtual Array* read(int64_t record, Array *dst=NULL) const = 0;

        virtual Array* iread(Array *dst=NULL) = 0;
        virtual Array* iread(int64_t record, Array *dst=NULL) = 0;

        virtual ostream& print(ostream &os) const;

        virtual int64_t get_bytes() const;

        /**
         * If there is a Mask over the record Dimension, translate the given
         * record from the index space of the mask into the index space of the
         * whole data.
         *
         * It is safe to call even if no masks are set in which case the
         * record is returned unchanged.
         *
         * @param[in] record the record to translate
         * @return the translated record
         */
        virtual int64_t translate_record(int64_t record) const;

        /**
         * Enables or disables record translation via translate_record().
         */
        virtual void set_translate_record(bool value);

    protected:
        AbstractVariable();

        /**
         * Return true if this Variable has any active Masks.
         *
         * @return true if this Variable requires subsetting
         */
        virtual bool needs_subset() const;

        /**
         * Return true if this Variable has any active, non-record Masks.
         *
         * @return true if this Variable requires subsetting
         */
        virtual bool needs_subset_record() const;

        /**
         * If this is a topology Variable and its connected Dimension has been
         * subset, we must renumber this Variable's values.
         *
         * @return true if this Variable should be renumbered
         */
        virtual bool needs_renumber() const;

        /**
         * If this is a topology Variable and its connected Dimension has been
         * subset, go ahead and renumber the given array assuming it was
         * something that was read earlier.
         *
         * @param[in] array the Array to renumber
         */
        virtual void renumber(Array *array) const;

        Array* read_alloc() const;
        Array* read_alloc(int64_t record) const;

        Array* iread_alloc();
        Array* iread_alloc(int64_t record);

        bool enable_record_translation;
};

#endif // ABSTRACTVARIABLE_H_
