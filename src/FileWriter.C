#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>
#include <string>
#include <vector>

#include "Error.H"
#include "FileWriter.H"

using std::ostream;
using std::string;
using std::vector;


vector<FileWriter::writer_t> FileWriter::writers;


FileWriter* FileWriter::open(const string &filename, FileFormat format)
{
    FileWriter *writer = NULL;
    vector<writer_t>::iterator it = FileWriter::writers.begin();
    vector<writer_t>::iterator end = FileWriter::writers.end();

    for (; it != end; ++it) {
        writer = (*it)(filename, format);
        if (NULL != writer) {
            break;
        }
    }
    if (NULL == writer) {
        ERR("unable to create writer -- no handler found");
    }

    return writer;
}


void FileWriter::register_writer(writer_t writer)
{
    writers.push_back(writer);
}


FileWriter::FileWriter()
{
}


FileWriter::~FileWriter()
{
}


ostream& operator << (ostream &os, const FileWriter *writer)
{
    return writer->print(os);
}
