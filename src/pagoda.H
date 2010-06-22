/**
 * @mainpage
 *
 * @section Introduction Introdcution
 *
 * TODO
 *
 * @section Installation Installation
 *
 * TODO
 *
 */
#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "AggregationUnion.H"
#include "Array.H"
#include "Attribute.H"
#include "CommandLineParser.H"
#include "Common.H"
#include "Dataset.H"
#include "DataType.H"
#include "Debug.H"
#include "Dimension.H"
#include "Error.H"
#include "FileWriter.H"
#include "Grid.H"
#include "IndexOutOfBoundsException.H"
#include "LatLonBox.H"
#include "Mask.H"
#include "MaskMap.H"
#include "NotImplementedException.H"
#include "Pack.H"
#include "RangeException.H"
#include "Slice.H"
#include "StringComparator.H"
#include "SubsetterCommands.H"
#include "SubsetterException.H"
#include "Timing.H"
#include "TypedValues.H"
#include "Util.H"
#include "Values.H"
#include "Variable.H"

namespace pagoda {

void initialize(int *argc, char ***argv);
void finalize();

};
