#ifndef INDEXHYPERSLAB_H_
#define INDEXHYPERSLAB_H_

#include <stdint.h>

#include <iostream>
#include <string>

using std::ostream;;
using std::string;

/**
 * Represents an index hyperslab for subsetting.
 */
class IndexHyperslab
{
    public:
        IndexHyperslab();
        IndexHyperslab(const string &name, int64_t index);
        IndexHyperslab(const string &name, int64_t min, int64_t max);
        IndexHyperslab(const string &name,
                       int64_t min, int64_t max, int64_t stride);
        IndexHyperslab(string arg);
        IndexHyperslab(const IndexHyperslab &that);
        ~IndexHyperslab();

        string get_name() const;
        int64_t get_min() const;
        int64_t get_max() const;
        int64_t get_stride() const;
        bool has_min() const;
        bool has_max() const;
        bool has_stride() const;

        bool operator== (const IndexHyperslab &that) const;
        IndexHyperslab& operator= (const IndexHyperslab &that);
        friend ostream& operator<< (ostream &os,
                                    const IndexHyperslab &hyperslab);

    protected:
        string name;
        int64_t min;
        int64_t max;
        int64_t stride;
        bool min_is_set;
        bool max_is_set;
        bool stride_is_set;
};

#endif /* INDEXHYPERSLAB_H_ */
