/**
 * @mainpage
 *
 * @section Introduction
 *
 * pagoda is both an API for data-parallel analysis of data as well as a set
 * of command-line tools which mimic the interface and semantics of the NetCDF
 * Operators (http://nco.sourceforge.net/).  This documentation refers to the
 * API only.  For details about the NCO-like tools, please see the README in
 * the distrubution.
 *
 * @section Installation
 *
 * pagoda depends on the Message Passing Interface, the Global Arrays toolkit
 * (http://www.emsl.pnl.gov/docs/global/) and parallel netcdf
 * (http://trac.mcs.anl.gov/projects/parallel-netcdf).
 *
 * See INSTALL and README in the distribution for details.
 * Note that we use the autotools to build pagoda, so those familiar with
 * running configure and make should start with configure --help.
 */
#ifndef _PAGODA_H_
#define _PAGODA_H_
#include "AbstractArray.H"
#include "AbstractDataset.H"
#include "AbstractVariable.H"
#include "AggregationDimension.H"
#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "AggregationUnion.H"
#include "AggregationVariable.H"
#include "AllNodeArray.H"
#include "Array.H"
#include "Attribute.H"
#include "Bootstrap.H"
#include "CommandLineOption.H"
#include "CommandLineParser.H"
#include "Common.H"
#include "Copy.H"
#include "CoordHyperslab.H"
#include "Dataset.H"
#include "DataType.H"
#include "Dimension.H"
#include "Error.H"
#include "FileWriter.H"
#include "GenericCommands.H"
#include "GeoGrid.H"
#include "Grid.H"
#include "IndexHyperslab.H"
#include "LatLonBox.H"
#include "Mask.H"
#include "MaskMap.H"
#include "NodeZeroArray.H"
#include "NotImplementedException.H"
#include "Pack.H"
#include "PagodaException.H"
#include "PgboCommands.H"
#include "PgeaCommands.H"
#include "PgraCommands.H"
#include "ProcessGroup.H"
#include "RangeException.H"
#include "RegularGrid.H"
#include "ScalarArray.H"
#include "Slice.H"
#include "StringComparator.H"
#include "SubsetterCommands.H"
#include "TypedValues.H"
#include "Util.H"
#include "Validator.H"
#include "ValidMax.H"
#include "ValidMin.H"
#include "ValidRange.H"
#include "Values.H"
#include "Variable.H"
#endif /* _PAGODA_H_ */
