#ifndef __PARALLEL_DEFS_H__
#define __PARALLEL_DEFS_H__

#include "mpi.h"

#ifndef MPI_UNSIGNED_LONG_LONG
#define MPI_UNSIGNED_LONG_LONG MPI_LONG_LONG
#endif // MPI_UNSIGNED_LONG_LONG

const MPI_Datatype SIZE_MPI_DATATYPE = (sizeof(size_t) > 4) ? MPI_UNSIGNED_LONG_LONG : MPI_UNSIGNED;

namespace auto_parallel
{
    typedef size_t task_id;
    typedef size_t message_id;
}

#endif // __PARALLEL_DEFS_H__
